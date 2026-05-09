use std::collections::BinaryHeap;

use async_stream::try_stream;
use futures::stream::Stream;
use futures::stream::StreamExt;

use crate::IteratorEither;

pub fn merge_sorted2<'a, T, I0, I1>(i0: I0, i1: I1) -> impl Iterator<Item = T> + 'a
where
    T: Ord + 'a,
    I0: Iterator<Item = T> + 'a,
    I1: Iterator<Item = T> + 'a,
{
    merge_sorted(vec![IteratorEither::Left(i0), IteratorEither::Right(i1)])
}

pub fn merge_sorted<'a, T: Ord + 'a>(
    mut iters: Vec<impl Iterator<Item = T> + 'a>,
) -> impl Iterator<Item = T> + 'a {
    let mut h: BinaryHeap<(std::cmp::Reverse<T>, usize)> = BinaryHeap::new();
    h.reserve_exact(iters.len());
    for (i, it) in iters.iter_mut().enumerate() {
        if let Some(t) = it.next() {
            h.push((std::cmp::Reverse(t), i));
        }
    }
    std::iter::from_fn(move || {
        let (t, i) = h.pop()?;
        if let Some(t) = iters[i].next() {
            h.push((std::cmp::Reverse(t), i));
        }
        Some(t.0)
    })
}

pub fn merge_sorted_streams<T: Ord + Send>(
    mut streams: Vec<impl Stream<Item = anyhow::Result<T>> + Unpin + Send>,
) -> impl Stream<Item = anyhow::Result<T>> + Send {
    try_stream! {
        let mut h: BinaryHeap<(std::cmp::Reverse<T>, usize)> = BinaryHeap::new();
        h.reserve_exact(streams.len());
        for (i, stream) in streams.iter_mut().enumerate() {
            if let Some(t) = stream.next().await.transpose()? {
                h.push((std::cmp::Reverse(t), i));
            }
        }
        while let Some((t, i)) = h.pop() {
            if let Some(t) = streams[i].next().await.transpose()? {
                h.push((std::cmp::Reverse(t), i));
            }
            yield t.0;
        }
    }
}
