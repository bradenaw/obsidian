use std::future::Future;

use futures::stream::StreamExt;

pub(crate) async fn wait_all<I: Iterator<Item: Future<Output = ()>>>(iter: I) {
    let mut waits = futures::stream::FuturesUnordered::new();
    for item in iter {
        waits.push(item);
    }
    while let Some(_) = waits.next().await {}
}
