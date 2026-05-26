use std::time::Duration;

use async_stream::stream;
use futures::Stream;
use tokio::time::sleep_until;
use tokio::time::Instant;

/// Returns a stream that yields an item every x += 50%.
pub fn jittered_ticker(x: Duration) -> impl Stream<Item = ()> {
    let mut next = Instant::now();
    stream! {
        loop {
            next += rand::random_range(x / 2..x * 3/2);
            yield ();
            sleep_until(next).await;
        }
    }
}
