// MIT License

// Copyright (c) 2016 Jerome Froelich

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! A minimal implementation of consistent hashing as described in [Consistent
//! Hashing and Random Trees: Distributed Caching Protocols for Relieving Hot
//! Spots on the World Wide Web] (https://www.akamai.com/es/es/multimedia/documents/technical-publication/consistent-hashing-and-random-trees-distributed-caching-protocols-for-relieving-hot-spots-on-the-world-wide-web-technical-publication.pdf).
//! Clients can use the `HashRing` struct to add consistent hashing to their
//! applications. `HashRing`'s API consists of three methods: `add`, `remove`,
//! and `get` for adding a node to the ring, removing a node from the ring, and
//! getting the node responsible for the provided key.
//!
//! ## Example
//!
//! Below is a simple example of how an application might use `HashRing` to make
//! use of consistent hashing. Since `HashRing` exposes only a minimal API
//! clients can build other abstractions, such as virtual nodes, on top of it.
//! The example below shows one potential implementation of virtual nodes on top
//! of `HashRing`
//!
//! ``` rust,no_run
//! extern crate hashring;
//!
//! use {
//!     hashring::HashRing,
//!     std::{
//!         net::{IpAddr, SocketAddr},
//!         str::FromStr,
//!     },
//! };
//!
//! #[derive(Debug, Copy, Clone, Hash, PartialEq)]
//! struct VNode {
//!     id: usize,
//!     addr: SocketAddr,
//! }
//!
//! impl VNode {
//!     fn new(ip: &str, port: u16, id: usize) -> Self {
//!         let addr = SocketAddr::new(IpAddr::from_str(&ip).unwrap(), port);
//!         VNode { id, addr }
//!     }
//! }
//!
//! fn main() {
//!     let mut ring: HashRing<VNode> = HashRing::new();
//!
//!     let mut nodes = vec![];
//!     nodes.push(VNode::new("127.0.0.1", 1024, 1));
//!     nodes.push(VNode::new("127.0.0.1", 1024, 2));
//!     nodes.push(VNode::new("127.0.0.2", 1024, 1));
//!     nodes.push(VNode::new("127.0.0.2", 1024, 2));
//!     nodes.push(VNode::new("127.0.0.2", 1024, 3));
//!     nodes.push(VNode::new("127.0.0.3", 1024, 1));
//!
//!     for node in nodes {
//!         ring.add_node(node).unwrap();
//!     }
//!
//!     println!("{:?}", ring.get_by_hash(&"foo").unwrap().data());
//!     println!("{:?}", ring.get_by_hash(&"bar").unwrap().data());
//!     println!("{:?}", ring.get_by_hash(&"baz").unwrap().data());
//! }
//! ```

use {
    range::KeyRange,
    siphasher::sip::SipHasher,
    std::hash::{BuildHasher, Hash, Hasher},
};

pub mod range;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("Duplicate node")]
    DuplicateNode,

    #[error("Node not found")]
    NodeNotFound,
}

pub trait RingHasher: BuildHasher + Clone {
    type Key: Clone + PartialEq + Eq + PartialOrd + Ord;

    fn get_key<T: Hash>(&self, input: T) -> Self::Key;
}

/// Default hash builder. Based on `SipHasher`, which produces 64-bit hashes.
#[derive(Clone)]
pub struct DefaultHashBuilder;

impl BuildHasher for DefaultHashBuilder {
    type Hasher = SipHasher;

    fn build_hasher(&self) -> Self::Hasher {
        SipHasher::new()
    }
}

impl RingHasher for DefaultHashBuilder {
    type Key = u64;

    fn get_key<T: Hash>(&self, input: T) -> Self::Key
    where
        T: Hash,
    {
        let mut hasher = self.build_hasher();
        input.hash(&mut hasher);
        hasher.finish()
    }
}

/// Node is an internal struct used to encapsulate the nodes that will be added
/// and removed from `HashRing`
#[derive(Debug, Clone, PartialEq, Eq)]
struct Node<K, T> {
    key: K,
    data: T,
}

impl<K, T> Node<K, T> {
    fn new(key: K, data: T) -> Self {
        Node { key, data }
    }
}

#[derive(Clone)]
pub struct HashRing<T, S: RingHasher = DefaultHashBuilder> {
    hash_builder: S,
    data: Vec<Node<S::Key, T>>,
}

impl<T> Default for HashRing<T> {
    fn default() -> Self {
        HashRing {
            hash_builder: DefaultHashBuilder,
            data: Vec::new(),
        }
    }
}

/// Hash Ring
///
/// A hash ring that provides consistent hashing for nodes that are added to it.
impl<T> HashRing<T> {
    /// Create a new `HashRing`.
    pub fn new() -> Self {
        Default::default()
    }
}

impl<T, S> HashRing<T, S>
where
    T: Hash,
    S: RingHasher,
{
    /// Creates an empty `HashRing` which will use the given hash builder.
    pub fn with_hasher(hash_builder: S) -> Self {
        HashRing {
            hash_builder,
            data: Vec::new(),
        }
    }

    /// Get the number of nodes in the hash ring.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the ring has no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Hashes `data` and returns its key into the hash ring.
    #[inline]
    pub fn key<U: Hash>(&self, data: &U) -> S::Key {
        self.hash_builder.get_key(data)
    }

    /// Adds `node` to the hash ring. Returns the new node's index, or an error
    /// if the hash ring already contains the node.
    pub fn add_node(&mut self, node: T) -> Result<usize, Error> {
        let key = self.key(&node);

        let Err(index) = self.find_node(&key) else {
            return Err(Error::DuplicateNode);
        };

        self.data.insert(index, Node::new(key, node));

        Ok(index)
    }

    /// Similar to `add_node()`, but doesn't check for duplicate nodes, and
    /// requires to be sorted after all of the nodes are added.
    pub fn add_node_unchecked(&mut self, node: T) {
        let key = self.key(&node);
        self.data.push(Node::new(key, node));
    }

    /// Sorts the ring. This is required after adding nodes with
    /// `add_node_unchecked()`.
    pub fn sort(&mut self) {
        self.data.sort_by(|a, b| a.key.cmp(&b.key))
    }

    /// Removes `node` from the hash ring. Returns an `Error` if the hash ring
    /// does not contain the `node`.
    pub fn remove_node(&mut self, node: &T) -> Result<(), Error> {
        let key = self.key(node);

        self.find_node(&key)
            .map(|idx| {
                self.data.remove(idx);
            })
            .map_err(|_| Error::NodeNotFound)
    }

    /// Returns the `NodeRef` for the node containing `key`, or an error if the
    /// hash ring is empty.
    #[inline]
    pub fn get_by_hash<U: Hash>(&self, key: &U) -> Result<NodeRef<'_, T, S>, Error> {
        self.get_by_key(&self.key(key))
    }

    /// Returns the `NodeRef` for the node containing `key`, or an error if the
    /// hash ring is empty.
    #[inline]
    pub fn get_by_key(&self, key: &S::Key) -> Result<NodeRef<'_, T, S>, Error> {
        if self.data.is_empty() {
            return Err(Error::NodeNotFound);
        }

        let index = match self.find_node(key) {
            Err(index) => index,
            Ok(index) => index,
        };

        let index = if index == self.data.len() { 0 } else { index };

        self.get_by_index(index)
    }

    /// Returns the `NodeRef` by node index within the hash ring, or an error if
    /// the hash ring is empty.
    #[inline]
    pub fn get_by_index(&self, index: usize) -> Result<NodeRef<'_, T, S>, Error> {
        if index < self.len() {
            Ok(NodeRef { ring: self, index })
        } else {
            Err(Error::NodeNotFound)
        }
    }

    /// Searches the ring for `node` and returns its `NodeRef`, or an error if
    /// the node is not found.
    #[inline]
    pub fn node(&self, node: &T) -> Result<NodeRef<'_, T, S>, Error> {
        if self.data.is_empty() {
            return Err(Error::NodeNotFound);
        }

        let key = self.key(node);

        let Ok(index) = self.find_node(&key) else {
            return Err(Error::NodeNotFound);
        };

        self.get_by_index(index)
    }

    pub fn iter(&self, start: impl Into<Option<S::Key>>) -> Iter<'_, T, S> {
        let start_node = if let Some(start_key) = start.into() {
            self.get_by_key(&start_key)
        } else {
            self.get_by_index(0)
        };

        start_node.map(Iter::new).unwrap_or(Iter::empty())
    }

    /// Internal method for traversing the hash ring.
    #[inline]
    fn find_node(&self, key: &S::Key) -> Result<usize, usize> {
        self.data.binary_search_by(|node| node.key.cmp(key))
    }

    /// Internal method for wrapping node index within the hash ring.
    #[inline]
    fn wrap_index(&self, index: usize) -> usize {
        index % self.data.len()
    }
}

/// Reference to a hash ring node. Acts as an iterator (using `prev()` and
/// `next()` methods), and provides additional node data like range and hash
/// key.
#[derive(Clone)]
pub struct NodeRef<'a, T, S: RingHasher> {
    ring: &'a HashRing<T, S>,
    index: usize,
}

impl<'a, T, S: RingHasher> std::fmt::Debug for NodeRef<'a, T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRef")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl<'a, T, S> NodeRef<'a, T, S>
where
    T: Hash,
    S: RingHasher,
{
    /// Returns the node's hash key.
    #[inline]
    pub fn key(&self) -> &S::Key {
        &self.node().key
    }

    /// Returns the node's data.
    #[inline]
    pub fn data(&self) -> &T {
        &self.node().data
    }

    /// Returns the previous node on the hash ring. If the hash ring contains
    /// only one node, the returned reference will be for the same node.
    #[inline]
    pub fn prev(&self) -> Self {
        let ring = self.ring;

        Self {
            ring,
            index: ring.wrap_index(ring.len() + self.index - 1),
        }
    }

    /// Returns the next node on the hash ring. If the hash ring contains
    /// only one node, the returned reference will be for the same node.
    #[inline]
    pub fn next(&self) -> Self {
        let ring = self.ring;

        Self {
            ring,
            index: ring.wrap_index(self.index + 1),
        }
    }

    /// Returns the nodes range on the hash ring.
    #[inline]
    pub fn range(&self) -> KeyRange<S::Key> {
        KeyRange {
            start: self.key().clone(),
            end: self.next().key().clone(),
        }
    }

    #[inline]
    fn node(&self) -> &Node<S::Key, T> {
        // Safe unwrap, since the node ref would not exist otherwise.
        self.ring.data.get(self.index).unwrap()
    }
}

pub struct Iter<'a, T, S: RingHasher> {
    start: usize,
    next: Option<NodeRef<'a, T, S>>,
}

impl<'a, T, S> Iter<'a, T, S>
where
    T: Hash,
    S: RingHasher,
{
    pub fn new(node: NodeRef<'a, T, S>) -> Self {
        Self {
            start: node.index,
            next: Some(node),
        }
    }

    pub fn empty() -> Self {
        Self {
            start: 0,
            next: None,
        }
    }
}

impl<'a, T, S> Iterator for Iter<'a, T, S>
where
    T: Hash,
    S: RingHasher,
{
    type Item = NodeRef<'a, T, S>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next.take();

        if let Some(current) = &current {
            let next = current.next();

            if next.index != self.start {
                self.next = Some(next);
            }
        }

        current
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{
            net::{IpAddr, SocketAddr},
            str::FromStr,
        },
    };

    #[derive(Debug, Copy, Clone, Hash, PartialEq)]
    struct VNode {
        id: usize,
        addr: SocketAddr,
    }

    impl VNode {
        fn new(ip: &str, port: u16, id: usize) -> Self {
            let addr = SocketAddr::new(IpAddr::from_str(&ip).unwrap(), port);
            VNode { id, addr }
        }
    }

    #[test]
    fn add_and_remove_nodes() {
        let mut ring: HashRing<VNode> = HashRing::new();

        assert_eq!(ring.len(), 0);
        assert!(ring.is_empty());

        let vnode1 = VNode::new("127.0.0.1", 1024, 1);
        let vnode2 = VNode::new("127.0.0.1", 1024, 2);
        let vnode3 = VNode::new("127.0.0.2", 1024, 1);

        ring.add_node(vnode1).unwrap();
        ring.add_node(vnode2).unwrap();
        ring.add_node(vnode3).unwrap();
        assert_eq!(ring.len(), 3);
        assert!(!ring.is_empty());

        ring.remove_node(&vnode2).unwrap();
        assert_eq!(ring.len(), 2);

        let vnode4 = VNode::new("127.0.0.2", 1024, 2);
        let vnode5 = VNode::new("127.0.0.2", 1024, 3);
        let vnode6 = VNode::new("127.0.0.3", 1024, 1);

        ring.add_node(vnode4).unwrap();
        ring.add_node(vnode5).unwrap();
        ring.add_node(vnode6).unwrap();

        ring.remove_node(&vnode1).unwrap();
        ring.remove_node(&vnode3).unwrap();
        ring.remove_node(&vnode6).unwrap();
        assert_eq!(ring.len(), 2);
    }

    #[test]
    fn get_nodes() {
        let mut ring: HashRing<VNode> = HashRing::new();

        assert!(matches!(
            ring.get_by_hash(&"foo"),
            Err(super::Error::NodeNotFound)
        ));

        let vnode1 = VNode::new("127.0.0.1", 1024, 1);
        let vnode2 = VNode::new("127.0.0.1", 1024, 2);
        let vnode3 = VNode::new("127.0.0.2", 1024, 1);
        let vnode4 = VNode::new("127.0.0.2", 1024, 2);
        let vnode5 = VNode::new("127.0.0.2", 1024, 3);
        let vnode6 = VNode::new("127.0.0.3", 1024, 1);

        ring.add_node(vnode1).unwrap();
        ring.add_node(vnode2).unwrap();
        ring.add_node(vnode3).unwrap();
        ring.add_node(vnode4).unwrap();
        ring.add_node(vnode5).unwrap();
        ring.add_node(vnode6).unwrap();

        assert_eq!(ring.get_by_hash(&"foo").unwrap().data(), &vnode5);
        assert_eq!(ring.get_by_hash(&"bar").unwrap().data(), &vnode3);
        assert_eq!(ring.get_by_hash(&"baz").unwrap().data(), &vnode5);

        assert_eq!(ring.get_by_hash(&"abc").unwrap().data(), &vnode2);
        assert_eq!(ring.get_by_hash(&"def").unwrap().data(), &vnode2);
        assert_eq!(ring.get_by_hash(&"ghi").unwrap().data(), &vnode6);

        assert_eq!(ring.get_by_hash(&"cat").unwrap().data(), &vnode1);
        assert_eq!(ring.get_by_hash(&"dog").unwrap().data(), &vnode5);
        assert_eq!(ring.get_by_hash(&"bird").unwrap().data(), &vnode5);

        // at least each node as a key
        let mut nodes = vec![0; 6];
        for x in 0..50_000 {
            let node = ring.get_by_hash(&x).unwrap();
            let node = node.data();
            if vnode1 == *node {
                nodes[0] += 1;
            }
            if vnode2 == *node {
                nodes[1] += 1;
            }
            if vnode3 == *node {
                nodes[2] += 1;
            }
            if vnode4 == *node {
                nodes[3] += 1;
            }
            if vnode5 == *node {
                nodes[4] += 1;
            }
            if vnode6 == *node {
                nodes[5] += 1;
            }
        }
        println!("{:?}", nodes);
        assert!(nodes.iter().all(|x| *x != 0));
    }

    #[test]
    fn advanced() {
        let mut ring: HashRing<VNode> = HashRing::new();

        let vnode1 = VNode::new("127.0.0.1", 1024, 1);
        let vnode2 = VNode::new("127.0.0.1", 1024, 2);

        // Ok: no nodes were affected.
        let result = ring.add_node(vnode1).unwrap();
        assert_eq!(result, 0);
        let vnode1_prev_range = ring.get_by_hash(&vnode1).unwrap().range();

        // Error: duplicates are not allowed.
        let result = ring.add_node(vnode1);
        assert_eq!(result.unwrap_err(), super::Error::DuplicateNode);

        let result = ring.add_node(vnode2).unwrap();
        assert_eq!(result, 1);
        let vnode1_curr_range = ring.get_by_hash(&vnode1).unwrap().range();

        let vnode2_curr_range = ring.get_by_hash(&vnode2).unwrap().range();

        assert_eq!(vnode1_prev_range.start, vnode1_curr_range.start);
        assert_eq!(vnode1_prev_range.end, vnode2_curr_range.end);
        assert_eq!(vnode1_curr_range.end, vnode2_curr_range.start);
    }

    #[test]
    fn node_ref() {
        let mut ring: HashRing<VNode> = HashRing::new();

        let vnode1 = VNode::new("127.0.0.1", 1024, 1);
        let vnode2 = VNode::new("127.0.0.1", 1024, 2);

        assert!(matches!(
            ring.get_by_hash(&vnode1),
            Err(super::Error::NodeNotFound)
        ));

        ring.add_node(vnode1).unwrap();

        let node_ref = ring.get_by_hash(&vnode1).unwrap();
        assert_eq!(node_ref.data(), &vnode1);
        assert_eq!(node_ref.index, 0);
        assert_eq!(node_ref.prev().index, 0);
        assert_eq!(node_ref.next().index, 0);

        ring.add_node(vnode2).unwrap();

        let node_ref = ring.get_by_hash(&vnode2).unwrap();
        assert_eq!(node_ref.data(), &vnode2);
        assert_eq!(node_ref.prev().data(), &vnode1);
        assert_eq!(node_ref.next().data(), &vnode1);
    }

    #[test]
    fn node_range() {
        // One node.
        {
            let mut ring: HashRing<VNode> = HashRing::new();
            let node = VNode::new("127.0.0.1", 1024, 1);
            let node_key = ring.key(&node);

            ring.add_node(node).unwrap();

            let range = ring.get_by_hash(&node).unwrap().range();
            assert_eq!(range, KeyRange {
                start: node_key,
                end: node_key
            });
        }

        // Multiple nodes.
        {
            let mut ring: HashRing<VNode> = HashRing::new();
            let node1 = VNode::new("127.0.0.1", 1024, 1);
            let node2 = VNode::new("127.0.0.1", 1024, 2);
            let node_key1 = ring.key(&node1);
            let node_key2 = ring.key(&node2);

            ring.add_node(node1).unwrap();
            ring.add_node(node2).unwrap();

            let range = ring.get_by_hash(&node1).unwrap().range();
            assert_eq!(range, KeyRange {
                start: node_key1,
                end: node_key2
            });

            let range = ring.get_by_hash(&node2).unwrap().range();
            assert_eq!(range, KeyRange {
                start: node_key2,
                end: node_key1
            });
        }

        // Invalid node.
        {
            let mut ring: HashRing<VNode> = HashRing::new();
            let node1 = VNode::new("127.0.0.1", 1024, 1);
            let node2 = VNode::new("127.0.0.1", 1024, 2);

            ring.add_node(node1).unwrap();

            let result = ring.node(&node2);
            assert!(matches!(result, Err(super::Error::NodeNotFound)));
        }
    }

    #[test]
    fn iter() {
        let node1 = VNode::new("127.0.0.1", 1024, 1);
        let node2 = VNode::new("127.0.0.1", 1024, 2);
        let node3 = VNode::new("127.0.0.1", 1024, 3);

        let mut ring: HashRing<VNode> = HashRing::new();
        assert!(ring.iter(None).next().is_none());

        ring.add_node(node1.clone()).unwrap();
        assert_eq!(ring.iter(None).next().unwrap().data(), &node1);

        ring.add_node(node2.clone()).unwrap();
        ring.add_node(node3.clone()).unwrap();

        let mut iter = ring.iter(Some(ring.key(&node2)));

        assert_eq!(iter.next().unwrap().data(), &node2);
        assert_eq!(iter.next().unwrap().data(), &node3);
        assert_eq!(iter.next().unwrap().data(), &node1);
        assert!(iter.next().is_none());
    }

    #[test]
    fn add_node_unchecked() {
        let node1 = VNode::new("127.0.0.1", 1024, 3);
        let node2 = VNode::new("127.0.0.1", 1024, 2);
        let node3 = VNode::new("127.0.0.1", 1024, 1);

        let mut ring1 = HashRing::new();
        ring1.add_node(node1.clone()).unwrap();
        ring1.add_node(node2.clone()).unwrap();
        ring1.add_node(node3.clone()).unwrap();

        let mut ring2 = HashRing::new();
        ring2.add_node_unchecked(node1.clone());
        ring2.add_node_unchecked(node2.clone());
        ring2.add_node_unchecked(node3.clone());
        ring2.sort();

        assert_eq!(ring1.data, ring2.data);
    }
}
