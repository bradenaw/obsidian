use std::future::Future;

use futures::stream::StreamExt;

pub(crate) async fn wait_all<I: Iterator<Item: Future<Output = ()>>>(iter: I) {
    let mut waits = futures::stream::FuturesUnordered::new();
    for item in iter {
        waits.push(item);
    }
    while let Some(_) = waits.next().await {}
}

pub(crate) async fn bounded_unordered_for_each<
    T,
    F: Fn(T) -> Fut,
    Fut: futures::Future<Output = ()>,
>(
    receiver: tokio::sync::mpsc::Receiver<T>,
    max_concurrent: usize,
    process: F,
) {
    let mut waits = futures::stream::FuturesUnordered::new();

    futures::pin_mut!(receiver);
    let mut done = false;
    loop {
        tokio::select! {
            next = receiver.recv(), if !done && waits.len() < max_concurrent => {
                match next {
                    Some(t) => {
                        waits.push(process(t));
                    },
                    None => {
                        done = true;
                    }
                }
            }
            Some(_) = waits.next() => {
                if done && waits.len() == 0 {
                    break;
                }
            }
        }
    }
}
