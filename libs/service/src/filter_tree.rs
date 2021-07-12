use std::borrow::Borrow;
use std::collections::hash_map::Iter as HashMapIter;
use std::collections::HashMap;
use std::hash::Hash;
use std::iter::Peekable;
use std::str::Split;

#[derive(Debug)]
struct Node<K, D> {
    hash_child: Option<Box<Node<K, D>>>,
    plus_child: Option<Box<Node<K, D>>>,
    named_children: HashMap<String, Node<K, D>>,
    data: HashMap<K, D>,
}

impl<K, D> Node<K, D> {
    #[inline]
    fn is_empty(&self) -> bool {
        self.hash_child.is_none()
            && self.plus_child.is_none()
            && self.named_children.is_empty()
            && self.data.is_empty()
    }
}

impl<K, D> Default for Node<K, D> {
    fn default() -> Self {
        Self {
            hash_child: None,
            plus_child: None,
            named_children: HashMap::new(),
            data: HashMap::new(),
        }
    }
}

pub struct FilterTree<K, D> {
    root: Node<K, D>,
    count: usize,
}

impl<K, D> Default for FilterTree<K, D> {
    fn default() -> Self {
        Self {
            root: Node::default(),
            count: 0,
        }
    }
}

impl<K: Eq + Hash, D> FilterTree<K, D> {
    #[inline]
    fn insert_value(
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
            Self::insert_value(segments, node, key, data)
        }
    }

    pub fn insert(&mut self, filter: impl AsRef<str>, key: K, data: D) -> Option<D> {
        let mut segments = filter.as_ref().split('/').peekable();
        assert!(segments.peek().is_some());
        let res = Self::insert_value(segments, &mut self.root, key, data);
        if res.is_none() {
            self.count += 1;
        }
        res
    }

    #[inline]
    fn matches_values<'a>(
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
                Self::matches_values(plus_node, nodes, tail);
            }
            if let Some(named_node) = parent_node.named_children.get(*segment) {
                Self::matches_values(named_node, nodes, tail);
            }
        }
    }

    pub fn matches(&self, topic: impl AsRef<str>) -> STreeIter<'_, K, D> {
        let mut nodes = Vec::new();
        let segments = topic.as_ref().split('/').collect::<Vec<_>>();
        assert!(!segments.is_empty());
        Self::matches_values(&self.root, &mut nodes, &segments[..]);
        STreeIter {
            nodes,
            current: None,
        }
    }

    pub fn is_matched(&self, topic: impl AsRef<str>) -> bool {
        self.matches(topic).next().is_some()
    }

    #[inline]
    fn remove_value<Q: ?Sized>(
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
            Self::remove_value(segments, node, key)
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
        let res = Self::remove_value(segments, &mut self.root, key);
        if res.is_some() {
            self.count -= 1;
        }
        res
    }

    #[inline]
    fn remove_all_value<Q: ?Sized>(parent_node: &mut Node<K, D>, key: &Q) -> usize
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
            remove_count += Self::remove_all_value(hash_node, key);
            if hash_node.is_empty() {
                parent_node.hash_child = None;
            }
        }

        if let Some(plus_node) = &mut parent_node.plus_child {
            if plus_node.data.remove(key).is_some() {
                remove_count += 1;
            }
            remove_count += Self::remove_all_value(plus_node, key);
            if plus_node.is_empty() {
                parent_node.plus_child = None;
            }
        }

        let mut remove_named = Vec::new();
        for (name, node) in &mut parent_node.named_children {
            if node.data.remove(key).is_some() {
                remove_count += 1;
            }
            remove_count += Self::remove_all_value(node, key);
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
        let count = Self::remove_all_value(&mut self.root, key);
        self.count -= count;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }
}

pub struct STreeIter<'a, K: 'a, D: 'a> {
    nodes: Vec<&'a Node<K, D>>,
    current: Option<HashMapIter<'a, K, D>>,
}

impl<'a, K: 'a, D: 'a> Iterator for STreeIter<'a, K, D> {
    type Item = (&'a K, &'a D);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.current {
                Some(hash_iter) => match hash_iter.next() {
                    Some(item) => return Some(item),
                    None => {
                        self.current = None;
                        continue;
                    }
                },
                None => {
                    if let Some(node) = self.nodes.pop() {
                        self.current = Some(node.data.iter());
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! do_matches {
        ($tree:expr, $topic:expr) => {{
            let mut res = $tree.matches($topic).collect::<Vec<_>>();
            res.sort_by(|a, b| a.0.cmp(&b.0));
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

        assert_eq!(tree.len(), 5);

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
        assert_eq!(tree.len(), 2);

        assert_eq!(tree.remove("a/b", &2), Some(1));
        assert_eq!(tree.len(), 1);
        assert!(!tree.root.named_children.is_empty());

        assert_eq!(tree.remove("a/b/c", &1), Some(1));
        assert_eq!(tree.len(), 0);

        assert!(tree.root.named_children.is_empty());

        tree.insert("a/+/c", 1, 1);
        tree.insert("a/b/c", 2, 1);
        assert_eq!(tree.len(), 2);
        assert_eq!(tree.remove("a/+/c", &1), Some(1));
        assert_eq!(tree.remove("a/b/c", &2), Some(1));
        assert_eq!(tree.len(), 0);
        assert!(tree.root.named_children.is_empty());

        tree.insert("a/#", 1, 1);
        tree.insert("a", 2, 1);
        assert_eq!(tree.len(), 2);
        assert_eq!(tree.remove("a/#", &1), Some(1));
        assert_eq!(tree.remove("a", &2), Some(1));
        assert_eq!(tree.len(), 0);
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
        assert_eq!(tree.len(), 3);

        tree.remove_all(&2);
        assert_eq!(tree.len(), 2);

        tree.remove_all(&3);
        assert_eq!(tree.len(), 1);

        tree.remove_all(&4);
        assert_eq!(tree.len(), 0);

        assert!(tree.root.is_empty());
    }
}
