//! Inode assignment over the engine's snapshots.
//!
//! The DB is path-keyed; FUSE is inode-keyed. `Tree` maps between them,
//! keeping inodes stable across snapshots for unchanged paths (new nodes
//! get the next free inode, removed nodes release theirs). Refreshed from
//! [`Store::snapshot`] — cheap, because only the generation number changed
//! cases skip the rebuild.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::Mutex;

use fuser::INodeNo;
use pds_core::store::RootSnapshot;

/// Virtual entry for the real `<root>/.pds` state dir, proxied so the CLI
/// control socket stays reachable through the mount.
pub const STATE_PROXY: &str = ".pds";

/// One servable node: snapshot data + its assigned inode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TNode {
    pub ino: INodeNo,
    pub is_dir: bool,
    pub size: u64,
    pub mtime: i64,
    pub content_ready: bool,
}

#[derive(Debug, Default)]
struct Inner {
    generation: u64,
    by_path: HashMap<String, TNode>,
    /// parent rel → children rels, sorted (root parent is "").
    children: HashMap<String, Vec<String>>,
    path_to_ino: HashMap<String, INodeNo>,
    ino_to_path: HashMap<INodeNo, String>,
    next_ino: u64,
}

pub struct Tree {
    inner: Mutex<Inner>,
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

impl Tree {
    pub fn new() -> Self {
        let mut inner = Inner {
            generation: 0,
            by_path: HashMap::new(),
            children: HashMap::new(),
            path_to_ino: HashMap::new(),
            ino_to_path: HashMap::new(),
            next_ino: 3, // 1 = root, 2 = .pds proxy
        };
        inner.path_to_ino.insert(String::new(), INodeNo::ROOT);
        inner.ino_to_path.insert(INodeNo::ROOT, String::new());
        inner.path_to_ino.insert(STATE_PROXY.to_string(), INodeNo(2));
        inner.ino_to_path.insert(INodeNo(2), STATE_PROXY.to_string());
        Self {
            inner: Mutex::new(inner),
        }
    }

    /// Re-index if the generation moved. Stable inos for surviving paths.
    pub fn refresh(&self, snap: &RootSnapshot) {
        let mut inner = self.inner.lock().expect("tree mutex poisoned");
        if snap.generation == inner.generation {
            return;
        }
        let mut by_path = HashMap::with_capacity(snap.by_path.len());
        for (rel, vnode) in &snap.by_path {
            let ino = match inner.path_to_ino.get(rel) {
                Some(ino) => *ino,
                None => {
                    let ino = INodeNo(inner.next_ino);
                    inner.next_ino += 1;
                    inner.path_to_ino.insert(rel.clone(), ino);
                    inner.ino_to_path.insert(ino, rel.clone());
                    ino
                }
            };
            by_path.insert(
                rel.clone(),
                TNode {
                    ino,
                    is_dir: vnode.is_dir,
                    size: vnode.size,
                    mtime: vnode.mtime,
                    content_ready: vnode.content_ready,
                },
            );
        }
        // Release inodes of vanished nodes.
        inner.path_to_ino.retain(|rel, _| {
            rel.is_empty() || rel == STATE_PROXY || snap.by_path.contains_key(rel)
        });
        let live: std::collections::HashSet<INodeNo> = inner.path_to_ino.values().copied().collect();
        inner
            .ino_to_path
            .retain(|ino, _| live.contains(ino));
        // State proxy shows up in listings under the root.
        let mut children = snap.children.clone();
        children
            .entry(String::new())
            .or_default()
            .push(STATE_PROXY.to_string());
        children
            .entry(String::new())
            .or_default()
            .sort();
        inner.by_path = by_path;
        inner.children = children;
        inner.generation = snap.generation;
    }

    fn rel_of_inner(inner: &Inner, ino: INodeNo) -> Option<String> {
        match ino {
            INodeNo::ROOT => Some(String::new()),
            _ => inner.ino_to_path.get(&ino).cloned(),
        }
    }

    /// Path for an inode; `""` is the root.
    pub fn rel_of(&self, ino: INodeNo) -> Option<String> {
        let inner = self.inner.lock().expect("tree mutex poisoned");
        Self::rel_of_inner(&inner, ino)
    }

    pub fn ino_of(&self, rel: &str) -> INodeNo {
        let inner = self.inner.lock().expect("tree mutex poisoned");
        if rel.is_empty() {
            return INodeNo::ROOT;
        }
        inner.path_to_ino.get(rel).copied().unwrap_or(INodeNo::ROOT)
    }

    /// Resolve `name` under `parent` to a child path.
    pub fn child_rel(&self, parent: INodeNo, name: &OsStr) -> Option<String> {
        let parent_rel = self.rel_of(parent)?;
        let name = name.to_str()?;
        if name.contains('/') || name == "." || name == ".." {
            return None;
        }
        Some(if parent_rel.is_empty() {
            name.to_string()
        } else {
            format!("{parent_rel}/{name}")
        })
    }

    /// Node data for a path, including the `.pds` synthetic directory and
    /// the root itself (`""` → synthetic root directory entry).
    pub fn node(&self, rel: &str) -> Option<TNode> {
        let inner = self.inner.lock().expect("tree mutex poisoned");
        if rel.is_empty() {
            return Some(TNode {
                ino: INodeNo::ROOT,
                is_dir: true,
                size: 0,
                mtime: 0,
                content_ready: true,
            });
        }
        if rel == STATE_PROXY {
            return Some(TNode {
                ino: INodeNo(2),
                is_dir: true,
                size: 0,
                mtime: 0,
                content_ready: true,
            });
        }
        inner.by_path.get(rel).cloned()
    }

    /// Children paths of a directory inode.
    pub fn children(&self, ino: INodeNo) -> Option<Vec<String>> {
        let inner = self.inner.lock().expect("tree mutex poisoned");
        let rel = Self::rel_of_inner(&inner, ino)?;
        if !rel.is_empty() && !inner.by_path.get(&rel)?.is_dir {
            return None; // not a directory
        }
        Some(inner.children.get(&rel).cloned().unwrap_or_default())
    }

    /// Inode of the parent of `ino` (root's parent is the root).
    pub fn parent_ino(&self, ino: INodeNo) -> INodeNo {
        let Ok(rel) = self.rel_of(ino).ok_or(()) else {
            return INodeNo::ROOT;
        };
        match rel.rsplit_once('/') {
            Some((parent, _)) => self.ino_of(parent),
            None => INodeNo::ROOT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pds_core::store::VNode;
    use std::collections::HashMap;

    fn vnode(rel: &str, is_dir: bool) -> VNode {
        let (parent, name) = match rel.rsplit_once('/') {
            Some((p, n)) => (p.to_string(), n.to_string()),
            None => (String::new(), rel.to_string()),
        };
        VNode {
            rel: rel.to_string(),
            parent,
            name,
            is_dir,
            size: 1,
            mtime: 0,
            content_ready: false,
        }
    }

    fn snap(generation: u64, nodes: &[&str]) -> RootSnapshot {
        let store = pds_core::Store::new();
        let mut map = HashMap::new();
        for (i, rel) in nodes.iter().enumerate() {
            let mut n = vnode(rel, i % 2 == 1);
            n.size = i as u64;
            map.insert(rel.to_string(), n);
        }
        store.publish_snapshot(map);
        let mut s = store.snapshot();
        s.generation = generation;
        s
    }

    #[test]
    fn inos_are_stable_across_refresh() {
        let tree = Tree::new();
        tree.refresh(&snap(1, &["a.txt", "dir"]));
        let ino_a = tree.node("a.txt").unwrap().ino;

        tree.refresh(&snap(2, &["a.txt", "dir", "dir/b.txt"]));
        assert_eq!(
            tree.node("a.txt").unwrap().ino,
            ino_a,
            "stable across generations"
        );
        assert!(tree.node("dir/b.txt").is_some());

        tree.refresh(&snap(3, &["dir", "dir/b.txt"]));
        assert!(tree.node("a.txt").is_none(), "vanished nodes are dropped");
        let b = tree.node("dir/b.txt").unwrap();
        assert_eq!(tree.rel_of(b.ino).as_deref(), Some("dir/b.txt"));
    }

    #[test]
    fn proxy_and_children() {
        let tree = Tree::new();
        tree.refresh(&snap(1, &["b.txt", "a.txt"]));
        let kids = tree.children(INodeNo::ROOT).unwrap();
        assert_eq!(kids[0], STATE_PROXY, ".pds sorts first");
        assert!(kids.contains(&"a.txt".to_string()));
        assert_eq!(tree.child_rel(INodeNo::ROOT, OsStr::new("a.txt")).as_deref(), Some("a.txt"));
        assert!(tree.child_rel(INodeNo::ROOT, OsStr::new("../etc")).is_none());
    }

    #[test]
    fn parent_ino_walks_up() {
        let tree = Tree::new();
        tree.refresh(&snap(1, &["dir", "dir/b.txt"]));
        let b = tree.node("dir/b.txt").unwrap();
        let dir = tree.node("dir").unwrap();
        assert_eq!(tree.parent_ino(b.ino), dir.ino);
        assert_eq!(tree.parent_ino(dir.ino), INodeNo::ROOT);
        assert_eq!(tree.parent_ino(INodeNo::ROOT), INodeNo::ROOT);
    }
}
