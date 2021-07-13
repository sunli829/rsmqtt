use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::iter::Peekable;
use std::str::Split;

use crate::Message;

#[derive(Debug)]
struct Node<K, D> {
    hash_child: Option<Box<Node<K, D>>>,
    plus_child: Option<Box<Node<K, D>>>,
    named_children: HashMap<String, Node<K, D>>,
    data: HashMap<K, D>,
    retained_message: Option<Message>,
}

impl<K, D> Node<K, D> {
    #[inline]
    fn is_empty(&self) -> bool {
        self.hash_child.is_none()
            && self.plus_child.is_none()
            && self.named_children.is_empty()
            && self.data.is_empty()
            && self.retained_message.is_none()
    }
}

impl<K, D> Default for Node<K, D> {
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

pub struct FilterTree<K, D> {
    root: Node<K, D>,
    subscribers_count: usize,
    retained_messages_count: usize,
    retained_messages_bytes: usize,
}

impl<K, D> Default for FilterTree<K, D> {
    fn default() -> Self {
        Self {
            root: Node::default(),
            subscribers_count: 0,
            retained_messages_count: 0,
            retained_messages_bytes: 0,
        }
    }
}

impl<K: Eq + Hash, D> FilterTree<K, D> {
    fn internal_insert(
        mut segments: Peekable<Split<char>>,
        parent_node: &mut Node<K, D>,
        key: K,
        data: D,
    ) -> Option<D> {
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
            node.data.insert(key, data)
        } else {
            Self::internal_insert(segments, node, key, data)
        }
    }

    pub fn insert(&mut self, filter: impl AsRef<str>, key: K, data: D) -> Option<D> {
        let mut segments = filter.as_ref().split('/').peekable();
        assert!(segments.peek().is_some());
        let res = Self::internal_insert(segments, &mut self.root, key, data);
        if res.is_none() {
            self.subscribers_count += 1;
        }
        res
    }

    fn internal_matches<'a>(
        parent_node: &'a Node<K, D>,
        nodes: &mut Vec<&'a Node<K, D>>,
        segments: &[&str],
    ) {
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

    pub fn matches(&self, topic: impl AsRef<str>) -> impl Iterator<Item = (&K, &D)> {
        let mut nodes = Vec::new();
        let segments = topic.as_ref().split('/').collect::<Vec<_>>();
        assert!(!segments.is_empty());
        Self::internal_matches(&self.root, &mut nodes, &segments[..]);
        nodes.into_iter().map(|node| node.data.iter()).flatten()
    }

    fn internal_remove<Q: ?Sized>(
        mut segments: Peekable<Split<char>>,
        parent_node: &mut Node<K, D>,
        key: &Q,
    ) -> Option<D>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let segment = segments.next().unwrap();
        let is_end = segments.peek().is_none();

        let node = match segment {
            "#" => parent_node.hash_child.as_deref_mut(),
            "+" => parent_node.plus_child.as_deref_mut(),
            _ => parent_node.named_children.get_mut(segment),
        }?;

        let res = if is_end {
            node.data.remove(key)
        } else {
            Self::internal_remove(segments, node, key)
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

    pub fn remove<Q: ?Sized>(&mut self, filter: impl AsRef<str>, key: &Q) -> Option<D>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut segments = filter.as_ref().split('/').peekable();
        assert!(segments.peek().is_some());
        let res = Self::internal_remove(segments, &mut self.root, key);
        if res.is_some() {
            self.subscribers_count -= 1;
        }
        res
    }

    fn internal_remove_all<Q: ?Sized>(parent_node: &mut Node<K, D>, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut remove_count = 0;

        if parent_node.data.remove(key).is_some() {
            remove_count += 1;
        }

        if let Some(hash_node) = &mut parent_node.hash_child {
            if hash_node.data.remove(key).is_some() {
                remove_count += 1;
            }
            remove_count += Self::internal_remove_all(hash_node, key);
            if hash_node.is_empty() {
                parent_node.hash_child = None;
            }
        }

        if let Some(plus_node) = &mut parent_node.plus_child {
            if plus_node.data.remove(key).is_some() {
                remove_count += 1;
            }
            remove_count += Self::internal_remove_all(plus_node, key);
            if plus_node.is_empty() {
                parent_node.plus_child = None;
            }
        }

        let mut remove_named = Vec::new();
        for (name, node) in &mut parent_node.named_children {
            if node.data.remove(key).is_some() {
                remove_count += 1;
            }
            remove_count += Self::internal_remove_all(node, key);
            if node.is_empty() {
                remove_named.push(name.to_string());
            }
        }

        for name in remove_named {
            parent_node.named_children.remove(&name);
        }

        remove_count
    }

    pub fn remove_all<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let count = Self::internal_remove_all(&mut self.root, key);
        self.subscribers_count -= count;
    }

    fn internal_matches_retained_messages_all<'a>(
        parent_node: &'a Node<K, D>,
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
        parent_node: &'a Node<K, D>,
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

    fn internal_set_retained_message<Q: ?Sized>(
        mut segments: Peekable<Split<char>>,
        parent_node: &mut Node<K, D>,
        retained_message: Option<Message>,
    ) -> Option<Message>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
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
    use codec::Qos;

    macro_rules! do_matches {
        ($tree:expr, $topic:expr) => {{
            let mut res = $tree.matches($topic).collect::<Vec<_>>();
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
        let mut tree = FilterTree::default();

        tree.insert("a/b/c", 1, 1);
        tree.insert("a/+/c", 2, 1);
        tree.insert("d/+", 1, 2);
        tree.insert("#", 3, 1);
        tree.insert("a/#", 4, 1);

        assert_eq!(tree.subscriber_count(), 5);

        assert_eq!(
            do_matches!(tree, "a/b/c"),
            vec![(&1, &1), (&2, &1), (&3, &1), (&4, &1)]
        );
        assert_eq!(do_matches!(tree, "d/1"), vec![(&1, &2), (&3, &1)]);
        assert_eq!(do_matches!(tree, "d/1/1"), vec![(&3, &1)]);
        assert_eq!(do_matches!(tree, "a/1"), vec![(&3, &1), (&4, &1)]);
    }

    #[test]
    fn test_remove() {
        let mut tree = FilterTree::default();

        tree.insert("a/b/c", 1, 1);
        tree.insert("a/b", 2, 1);
        assert_eq!(tree.subscriber_count(), 2);

        assert_eq!(tree.remove("a/b", &2), Some(1));
        assert_eq!(tree.subscriber_count(), 1);
        assert!(!tree.root.named_children.is_empty());

        assert_eq!(tree.remove("a/b/c", &1), Some(1));
        assert_eq!(tree.subscriber_count(), 0);

        assert!(tree.root.named_children.is_empty());

        tree.insert("a/+/c", 1, 1);
        tree.insert("a/b/c", 2, 1);
        assert_eq!(tree.subscriber_count(), 2);
        assert_eq!(tree.remove("a/+/c", &1), Some(1));
        assert_eq!(tree.remove("a/b/c", &2), Some(1));
        assert_eq!(tree.subscriber_count(), 0);
        assert!(tree.root.named_children.is_empty());

        tree.insert("a/#", 1, 1);
        tree.insert("a", 2, 1);
        assert_eq!(tree.subscriber_count(), 2);
        assert_eq!(tree.remove("a/#", &1), Some(1));
        assert_eq!(tree.remove("a", &2), Some(1));
        assert_eq!(tree.subscriber_count(), 0);
        assert!(tree.root.named_children.is_empty());
    }

    #[test]
    fn test_remove_all() {
        let mut tree = FilterTree::default();

        tree.insert("a/b/c", 1, 1);
        tree.insert("a/+/c", 2, 1);
        tree.insert("d/+", 1, 2);
        tree.insert("#", 3, 1);
        tree.insert("a/#", 4, 1);

        tree.remove_all(&1);
        assert_eq!(tree.subscriber_count(), 3);

        tree.remove_all(&2);
        assert_eq!(tree.subscriber_count(), 2);

        tree.remove_all(&3);
        assert_eq!(tree.subscriber_count(), 1);

        tree.remove_all(&4);
        assert_eq!(tree.subscriber_count(), 0);

        assert!(tree.root.is_empty());
    }

    #[test]
    fn test_retained_messages() {
        let mut tree = FilterTree::<i32, i32>::default();

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
