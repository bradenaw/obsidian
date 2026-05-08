use std::collections::HashMap;
use std::hash::Hash;
use std::iter;

pub(super) struct Graph<V, E> {
    edges: HashMap<V, HashMap<V, E>>,

    num_edges: usize,
}

impl<V, E> Graph<V, E>
where
    V: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
            num_edges: 0,
        }
    }

    /// Inserts or replaces the edge between src and dst with the given edge.
    pub fn insert(&mut self, src: V, edge: E, dst: V) {
        if !self.edges.contains_key(&dst) {
            self.edges.insert(dst.clone(), HashMap::new());
        }
        let inserted = self
            .edges
            .entry(src)
            .or_default()
            .insert(dst, edge)
            .is_none();
        if inserted {
            self.num_edges += 1;
        }
    }

    pub fn edge(&self, src: &V, dst: &V) -> Option<&E> {
        self.edges.get(src).and_then(|out_edges| out_edges.get(dst))
    }

    pub fn out_edges<'a>(&'a self, src: &'a V) -> impl Iterator<Item = (&'a V, &'a E)> + 'a {
        iter::from_coroutine(
            #[coroutine]
            || {
                if let Some(out_edges) = self.edges.get(src) {
                    for (dst, edge_type) in out_edges {
                        yield (dst, edge_type);
                    }
                }
            },
        )
    }

    pub fn vertices(&self) -> impl Iterator<Item = &V> {
        self.edges.keys()
    }

    pub fn num_vertices(&self) -> usize {
        self.edges.len()
    }

    pub fn num_edges(&self) -> usize {
        self.num_edges
    }
}
