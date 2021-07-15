use std::collections::HashMap;
use std::iter::Peekable;
use std::str::Split;

use indexmap::IndexMap;

use crate::filter_util::Filter;
use crate::storage::FilterItem;
use crate::Message;

#[derive(Debug)]
struct Node {
    hash_child: Option<Box<Node>>,
    plus_child: Option<Box<Node>>,
    named_children: HashMap<String, Node>,
    data: HashMap<String, FilterItem>,
    retained_message: Option<Message>,
}

impl Node {
    #[inline]
    fn is_empty(&self) -> bool {
        self.hash_child.is_none()
            && self.plus_child.is_none()
            && self.named_children.is_empty()
            && self.data.is_empty()
            && self.retained_message.is_none()
    }
}

impl Default for Node {
    fn default() -> Self {
        Self {
            hash_child: None,
            plus_child: None,
            named_children: HashMap::new(),
            data: HashMap::new(),
            retained_message: None,
        }
    }
}

pub struct Trie {
    root: Node,
    share_subscriptions: HashMap<String, Node>,
    subscribers_count: usize,
    retained_messages_count: usize,
    retained_messages_bytes: usize,
}

impl Default for Trie {
    fn default() -> Self {
        Self {
            root: Node::default(),
            share_subscriptions: HashMap::new(),
            subscribers_count: 0,
            retained_messages_count: 0,
            retained_messages_bytes: 0,
        }
    }
}

impl Trie {
    fn internal_subscribe(
        mut segments: Peekable<Split<char>>,
        parent_node: &mut Node,
        client_id: String,
        data: FilterItem,
    ) -> Option<FilterItem> {
        let segment = segments.next().unwrap();
        let is_end = segments.peek().is_none();

        let node = match segment {
            "#" => parent_node
                .hash_child
                .get_or_insert_with(|| Box::new(Node::default())),
            "+" => parent_node
                .plus_child
                .get_or_insert_with(|| Box::new(Node::default())),
            _ => parent_node
                .named_children
                .entry(segment.to_string())
                .or_default(),
        };

        if is_end {
            node.data.insert(client_id.to_string(), data)
        } else {
            Self::internal_subscribe(segments, node, client_id, data)
        }
    }

    pub fn subscribe(
        &mut self,
        filter: Filter<'_>,
        client_id: impl Into<String>,
        data: FilterItem,
    ) -> Option<FilterItem> {
        let segments = filter.path.split('/').peekable();
        let res = match filter.share_name {
            Some(share_name) => Self::internal_subscribe(
                segments,
                self.share_subscriptions
                    .entry(share_name.to_string())
                    .or_default(),
                client_id.into(),
                data,
            ),
            None => Self::internal_subscribe(segments, &mut self.root, client_id.into(), data),
        };
        if res.is_none() {
            self.subscribers_count += 1;
        }
        res
    }

    fn internal_matches<'a>(parent_node: &'a Node, nodes: &mut Vec<&'a Node>, segments: &[&str]) {
        let (segment, tail) = segments.split_first().unwrap();
        let is_end = tail.is_empty();

        nodes.extend(parent_node.hash_child.as_deref());

        if is_end {
            nodes.extend(parent_node.plus_child.as_deref());
            nodes.extend(parent_node.named_children.get(*segment));
        } else {
            if let Some(plus_node) = parent_node.plus_child.as_deref() {
                Self::internal_matches(plus_node, nodes, tail);
            }
            if let Some(named_node) = parent_node.named_children.get(*segment) {
                Self::internal_matches(named_node, nodes, tail);
            }
        }
    }

    pub fn matches(
        &self,
        topic: impl AsRef<str>,
    ) -> impl Iterator<Item = (&str, Vec<&FilterItem>)> {
        let segments = topic.as_ref().split('/').collect::<Vec<_>>();
        assert!(!segments.is_empty());

        let mut matched: HashMap<&str, Vec<&FilterItem>> = HashMap::new();

        let mut nodes = Vec::new();
        Self::internal_matches(&self.root, &mut nodes, &segments[..]);
        for (k, item) in nodes.iter().map(|node| node.data.iter()).flatten() {
            matched.entry(k).or_default().push(item);
        }

        matched.into_iter()
    }

    pub fn matches_shared(
        &self,
        topic: impl AsRef<str>,
    ) -> impl Iterator<Item = (&str, Vec<&FilterItem>)> {
        let segments = topic.as_ref().split('/').collect::<Vec<_>>();
        assert!(!segments.is_empty());

        let mut nodes = Vec::new();
        let mut matched: HashMap<&str, Vec<&FilterItem>> = HashMap::new();

        for node in self.share_subscriptions.values() {
            let mut share_matches: IndexMap<&str, Vec<&FilterItem>> = IndexMap::new();

            nodes.clear();
            Self::internal_matches(node, &mut nodes, &segments[..]);
            for (k, item) in nodes.iter().map(|node| node.data.iter()).flatten() {
                share_matches.entry(k).or_default().push(item);
            }

            if !share_matches.is_empty() {
                let (k, items) = share_matches
                    .swap_remove_index(fastrand::usize(0..share_matches.len()))
                    .unwrap();
                matched.entry(k).or_default().extend(items);
            }
        }

        matched.into_iter()
    }

    fn internal_unsubscribe(
        mut segments: Peekable<Split<char>>,
        parent_node: &mut Node,
        client_id: &str,
    ) -> Option<FilterItem> {
        let segment = segments.next().unwrap();
        let is_end = segments.peek().is_none();

        let node = match segment {
            "#" => parent_node.hash_child.as_deref_mut(),
            "+" => parent_node.plus_child.as_deref_mut(),
            _ => parent_node.named_children.get_mut(segment),
        }?;

        let res = if is_end {
            node.data.remove(client_id)
        } else {
            Self::internal_unsubscribe(segments, node, client_id)
        };

        if node.is_empty() {
            match segment {
                "#" => parent_node.hash_child = None,
                "+" => parent_node.plus_child = None,
                _ => {
                    parent_node.named_children.remove(segment);
                }
            }
        }

        res
    }

    pub fn unsubscribe(&mut self, filter: Filter<'_>, client_id: &str) -> Option<FilterItem> {
        let segments = filter.path.split('/').peekable();
        let res = match filter.share_name {
            Some(share_name) => Self::internal_unsubscribe(
                segments,
                self.share_subscriptions
                    .entry(share_name.to_string())
                    .or_default(),
                client_id,
            ),
            None => Self::internal_unsubscribe(segments, &mut self.root, client_id),
        };
        if res.is_some() {
            self.subscribers_count -= 1;
        }
        res
    }

    fn internal_unsubscribe_all(parent_node: &mut Node, client_id: &str) -> usize {
        let mut remove_count = 0;

        if parent_node.data.remove(client_id).is_some() {
            remove_count += 1;
        }

        if let Some(hash_node) = &mut parent_node.hash_child {
            if hash_node.data.remove(client_id).is_some() {
                remove_count += 1;
            }
            remove_count += Self::internal_unsubscribe_all(hash_node, client_id);
            if hash_node.is_empty() {
                parent_node.hash_child = None;
            }
        }

        if let Some(plus_node) = &mut parent_node.plus_child {
            if plus_node.data.remove(client_id).is_some() {
                remove_count += 1;
            }
            remove_count += Self::internal_unsubscribe_all(plus_node, client_id);
            if plus_node.is_empty() {
                parent_node.plus_child = None;
            }
        }

        let mut remove_named = Vec::new();
        for (name, node) in &mut parent_node.named_children {
            if node.data.remove(client_id).is_some() {
                remove_count += 1;
            }
            remove_count += Self::internal_unsubscribe_all(node, client_id);
            if node.is_empty() {
                remove_named.push(name.to_string());
            }
        }

        for name in remove_named {
            parent_node.named_children.remove(&name);
        }

        remove_count
    }

    pub fn unsubscribe_all(&mut self, client_id: &str) {
        let mut count = Self::internal_unsubscribe_all(&mut self.root, client_id);
        for node in self.share_subscriptions.values_mut() {
            count += Self::internal_unsubscribe_all(node, client_id);
        }
        self.subscribers_count -= count;
    }

    fn internal_matches_retained_messages_all<'a>(
        parent_node: &'a Node,
        msgs: &mut Vec<&'a Message>,
    ) {
        if let Some(msg) = &parent_node.retained_message {
            msgs.push(msg);
        }
        for child in parent_node.named_children.values() {
            Self::internal_matches_retained_messages_all(child, msgs);
        }
    }

    fn internal_matches_retained_messages<'a>(
        parent_node: &'a Node,
        msgs: &mut Vec<&'a Message>,
        segments: &[&str],
    ) {
        let (segment, tail) = segments.split_first().unwrap();
        let is_end = tail.is_empty();

        match *segment {
            "#" => {
                Self::internal_matches_retained_messages_all(parent_node, msgs);
            }
            "+" => {
                for child in parent_node.named_children.values() {
                    if is_end {
                        msgs.extend(child.retained_message.as_ref());
                    } else {
                        Self::internal_matches_retained_messages(child, msgs, tail);
                    }
                }
            }
            _ => {
                if let Some(child) = parent_node.named_children.get(*segment) {
                    if is_end {
                        msgs.extend(child.retained_message.as_ref());
                    } else {
                        Self::internal_matches_retained_messages(child, msgs, tail);
                    }
                }
            }
        }
    }

    pub fn matches_retained_messages(
        &self,
        topic: impl AsRef<str>,
    ) -> impl Iterator<Item = &Message> {
        let mut msgs = Vec::new();
        let segments = topic.as_ref().split('/').collect::<Vec<_>>();
        assert!(!segments.is_empty());
        Self::internal_matches_retained_messages(&self.root, &mut msgs, &segments[..]);
        msgs.into_iter()
    }

    fn internal_set_retained_message(
        mut segments: Peekable<Split<char>>,
        parent_node: &mut Node,
        retained_message: Option<Message>,
    ) -> Option<Message> {
        let segment = segments.next().unwrap();
        let is_end = segments.peek().is_none();
        let is_delete = retained_message.is_none();

        let node = parent_node
            .named_children
            .entry(segment.to_string())
            .or_default();

        let res = if is_end {
            let res = node.retained_message.take();
            node.retained_message = retained_message;
            res
        } else {
            Self::internal_set_retained_message(segments, node, retained_message)
        };

        if is_delete && node.is_empty() {
            parent_node.named_children.remove(segment);
        }

        res
    }

    pub fn set_retained_message(
        &mut self,
        path: impl AsRef<str>,
        msg: Option<Message>,
    ) -> Option<Message> {
        let mut segments = path.as_ref().split('/').peekable();
        assert!(segments.peek().is_some());
        let set_new = msg.is_some();
        let msg_size = msg
            .as_ref()
            .map(|msg| msg.payload().len())
            .unwrap_or_default();
        let res = Self::internal_set_retained_message(segments, &mut self.root, msg);
        match (&res, set_new) {
            (None, true) => {
                self.retained_messages_count += 1;
                self.retained_messages_bytes += msg_size;
            }
            (Some(dropped_msg), false) => {
                self.retained_messages_count -= 1;
                self.retained_messages_bytes -= dropped_msg.payload().len();
            }
            _ => {}
        }
        res
    }

    #[inline]
    pub fn subscriber_count(&self) -> usize {
        self.subscribers_count
    }

    #[inline]
    pub fn retained_messages_count(&self) -> usize {
        self.retained_messages_count
    }

    #[inline]
    pub fn retained_messages_bytes(&self) -> usize {
        self.retained_messages_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter_util::parse_filter;
    use codec::Qos;
    use std::convert::TryInto;

    macro_rules! item {
        ($id: expr) => {
            FilterItem {
                qos: Qos::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: codec::RetainHandling::OnEverySubscribe,
                id: Some($id.try_into().unwrap()),
            }
        };
    }

    macro_rules! do_matches {
        ($tree:expr, $topic:expr) => {{
            let mut res = $tree
                .matches($topic)
                .map(|(key, items)| {
                    items
                        .into_iter()
                        .map(move |item| (key, item.id.unwrap().get()))
                })
                .flatten()
                .collect::<Vec<_>>();
            res.sort_by(|a, b| a.0.cmp(&b.0));
            res
        }};
    }

    macro_rules! do_matche_retained_messages {
        ($tree:expr, $topic:expr) => {{
            let mut res = $tree
                .matches_retained_messages($topic)
                .map(|msg| &*msg.topic())
                .collect::<Vec<_>>();
            res.sort();
            res
        }};
    }

    #[test]
    fn test_matches() {
        let mut tree = Trie::default();

        tree.subscribe(parse_filter("a/b/c").unwrap(), "1", item!(1));
        tree.subscribe(parse_filter("a/+/c").unwrap(), "2", item!(1));
        tree.subscribe(parse_filter("d/+").unwrap(), "1", item!(2));
        tree.subscribe(parse_filter("#").unwrap(), "3", item!(1));
        tree.subscribe(parse_filter("a/#").unwrap(), "4", item!(1));

        assert_eq!(tree.subscriber_count(), 5);

        assert_eq!(
            do_matches!(tree, "a/b/c"),
            vec![("1", 1), ("2", 1), ("3", 1), ("4", 1)]
        );
        assert_eq!(do_matches!(tree, "d/1"), vec![("1", 2), ("3", 1)]);
        assert_eq!(do_matches!(tree, "d/1/1"), vec![("3", 1)]);
        assert_eq!(do_matches!(tree, "a/1"), vec![("3", 1), ("4", 1)]);
    }

    #[test]
    fn test_remove() {
        let mut tree = Trie::default();

        tree.subscribe(parse_filter("a/b/c").unwrap(), "1", item!(1));
        tree.subscribe(parse_filter("a/b").unwrap(), "2", item!(1));
        assert_eq!(tree.subscriber_count(), 2);

        assert_eq!(
            tree.unsubscribe(parse_filter("a/b").unwrap(), "2"),
            Some(item!(1))
        );
        assert_eq!(tree.subscriber_count(), 1);
        assert!(!tree.root.named_children.is_empty());

        assert_eq!(
            tree.unsubscribe(parse_filter("a/b/c").unwrap(), "1"),
            Some(item!(1))
        );
        assert_eq!(tree.subscriber_count(), 0);

        assert!(tree.root.named_children.is_empty());

        tree.subscribe(parse_filter("a/+/c").unwrap(), "1", item!(1));
        tree.subscribe(parse_filter("a/b/c").unwrap(), "2", item!(1));
        assert_eq!(tree.subscriber_count(), 2);
        assert_eq!(
            tree.unsubscribe(parse_filter("a/+/c").unwrap(), "1"),
            Some(item!(1))
        );
        assert_eq!(
            tree.unsubscribe(parse_filter("a/b/c").unwrap(), "2"),
            Some(item!(1))
        );
        assert_eq!(tree.subscriber_count(), 0);
        assert!(tree.root.named_children.is_empty());

        tree.subscribe(parse_filter("a/#").unwrap(), "1", item!(1));
        tree.subscribe(parse_filter("a").unwrap(), "2", item!(1));
        assert_eq!(tree.subscriber_count(), 2);
        assert_eq!(
            tree.unsubscribe(parse_filter("a/#").unwrap(), "1"),
            Some(item!(1))
        );
        assert_eq!(
            tree.unsubscribe(parse_filter("a").unwrap(), "2"),
            Some(item!(1))
        );
        assert_eq!(tree.subscriber_count(), 0);
        assert!(tree.root.named_children.is_empty());
    }

    #[test]
    fn test_remove_all() {
        let mut tree = Trie::default();

        tree.subscribe(parse_filter("a/b/c").unwrap(), "1", item!(1));
        tree.subscribe(parse_filter("a/+/c").unwrap(), "2", item!(1));
        tree.subscribe(parse_filter("d/+").unwrap(), "1", item!(2));
        tree.subscribe(parse_filter("#").unwrap(), "3", item!(1));
        tree.subscribe(parse_filter("a/#").unwrap(), "4", item!(1));

        tree.unsubscribe_all("1");
        assert_eq!(tree.subscriber_count(), 3);

        tree.unsubscribe_all("2");
        assert_eq!(tree.subscriber_count(), 2);

        tree.unsubscribe_all("3");
        assert_eq!(tree.subscriber_count(), 1);

        tree.unsubscribe_all("4");
        assert_eq!(tree.subscriber_count(), 0);

        assert!(tree.root.is_empty());
    }

    #[test]
    fn test_retained_messages() {
        let mut tree = Trie::default();

        tree.set_retained_message(
            "a/b/c",
            Some(Message::new("a", Qos::AtMostOnce, &b"123"[..])),
        );
        tree.set_retained_message(
            "a/k/c",
            Some(Message::new("d", Qos::AtMostOnce, &b"123"[..])),
        );
        tree.set_retained_message("a/b", Some(Message::new("b", Qos::AtMostOnce, &b"123"[..])));
        tree.set_retained_message("b/1", Some(Message::new("c", Qos::AtMostOnce, &b"123"[..])));
        assert_eq!(tree.retained_messages_count(), 4);

        assert_eq!(
            do_matche_retained_messages!(tree, "a/#"),
            vec!["a", "b", "d"]
        );
        assert_eq!(do_matche_retained_messages!(tree, "a/b"), vec!["b"]);
        assert_eq!(do_matche_retained_messages!(tree, "b/+"), vec!["c"]);
        assert_eq!(
            do_matche_retained_messages!(tree, "#"),
            vec!["a", "b", "c", "d"]
        );
        assert_eq!(do_matche_retained_messages!(tree, "a/+/c"), vec!["a", "d"]);

        tree.set_retained_message("b/1", None);
        assert_eq!(tree.retained_messages_count(), 3);

        tree.set_retained_message("a/b", None);
        assert_eq!(tree.retained_messages_count(), 2);

        tree.set_retained_message("c", None);
        assert_eq!(tree.retained_messages_count(), 2);

        tree.set_retained_message("a/b/c", None);
        assert_eq!(tree.retained_messages_count(), 1);

        tree.set_retained_message("a/k/c", None);
        assert_eq!(tree.retained_messages_count(), 0);

        assert!(tree.root.is_empty());
    }
}
