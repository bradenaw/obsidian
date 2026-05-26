use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::time::Duration;

use async_stream::stream;
use futures::Stream;
use futures::StreamExt;

pub fn delay_for_retry(i: usize, min_delay: Duration, max_delay: Duration) -> Duration {
    let avg_delay = std::cmp::min(
        min_delay.saturating_mul(2u32.saturating_pow(i as u32)),
        max_delay,
    );
    rand::random_range(avg_delay / 2..avg_delay * 3 / 2)
}

pub async fn sleep_for_retry(i: usize, min_delay: Duration, max_delay: Duration) {
    tokio::time::sleep(delay_for_retry(i, min_delay, max_delay)).await;
}

pub struct Retry {
    min_delay: Duration,
    max_delay: Duration,
    timeout: Duration,
    n_attempts: usize,
}

pub enum RetryResult<T, E> {
    Ok(T),
    Retry(E),
    Err(E),
}

impl<T, E> From<Result<T, E>> for RetryResult<T, E> {
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(v) => RetryResult::Ok(v),
            Err(e) => RetryResult::Err(e),
        }
    }
}

impl Default for Retry {
    fn default() -> Self {
        Self::new()
    }
}

impl Retry {
    pub fn new() -> Self {
        Self {
            min_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(5000),
            timeout: Duration::MAX,
            n_attempts: 5,
        }
    }

    pub fn min_delay(mut self, x: Duration) -> Self {
        self.min_delay = x;
        self
    }

    pub fn max_delay(mut self, x: Duration) -> Self {
        self.max_delay = x;
        self
    }

    pub fn timeout(mut self, x: Duration) -> Self {
        self.timeout = x;
        self
    }

    pub fn n_attempts(mut self, x: usize) -> Self {
        self.n_attempts = x;
        self
    }

    pub async fn indefinitely<
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        T,
        E: Deref<Target = dyn std::error::Error + Send + Sync + 'static>,
    >(
        self,
        f: &F,
    ) -> T {
        let mut delays = RetryDelay::new(self.min_delay, self.max_delay);
        loop {
            match f().await {
                Ok(t) => {
                    return t;
                }
                Err(e) => {
                    let delay = delays.next();
                    log::warn!("error, retrying in {:?}: {:?}", delay, e.deref());
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub async fn with_retry<
        F: Fn() -> Fut,
        Fut: Future<Output = RetryResult<T, E>>,
        T,
        E: std::error::Error + Send + Sync + 'static,
    >(
        self,
        f: F,
    ) -> anyhow::Result<T> {
        let start = std::time::Instant::now();
        let mut last_err = None;

        let mut delays = RetryDelay::new(self.min_delay, self.max_delay);

        for _ in 0..self.n_attempts {
            match tokio::time::timeout(self.timeout - start.elapsed(), f()).await {
                Ok(RetryResult::Ok(t)) => return Ok(t),
                Ok(RetryResult::Retry(e)) => {
                    last_err = Some(e);
                }
                Ok(RetryResult::Err(e)) => {
                    return Err(e.into());
                }
                Err(_) => {
                    // Timeout. Bubble out the last actual error if possible.
                    if let Some(e) = last_err {
                        return Err(e.into());
                    }
                    anyhow::bail!("timed out")
                }
            }

            let delay = delays.next();
            if delay > self.timeout - start.elapsed() {
                // last_err can't be None here, if it were we would have already returned.
                return Err(last_err.unwrap().into());
            }
            log::warn!("error, retrying in {:?}: {:?}", delay, last_err);
            tokio::time::sleep(delay).await;
        }
        if let Some(e) = last_err {
            return Err(e.into());
        }
        anyhow::bail!("no attempts")
    }

    pub fn retry_stream_indefinitely<'a, F, S, Item>(
        self,
        mut f: F,
    ) -> impl Stream<Item = Item> + Send + Unpin + 'a
    where
        F: FnMut() -> S + Send + Sync + 'a,
        S: Stream<Item = anyhow::Result<Item>> + Send + Unpin,
        Item: Send + 'static,
    {
        Box::pin(stream! {
            let mut retry_delay = RetryDelay::new(self.min_delay, self.max_delay);
            loop {
                let mut s = f();
                loop {
                    match s.next().await {
                        Some(Ok(item)) => {
                            yield item;
                        },
                        Some(Err(e)) => {
                            let delay = retry_delay.next();
                            log::warn!("error in stream, retrying in {:?}: {:?}", delay, e);
                            tokio::time::sleep(delay).await;
                            break;
                        },
                        None => {
                            return;
                        }
                    }
                }
            }
        })
    }
}

struct RetryDelay {
    attempts: VecDeque<std::time::Instant>,
    size: usize,
    max_age: Duration,
    min_delay: Duration,
    max_delay: Duration,
}

impl RetryDelay {
    fn new(min_delay: Duration, max_delay: Duration) -> Self {
        // We only need this many for delay_for_retry to return the max value, so any more are
        // pointless.
        let size =
            (max_delay.as_secs_f64().log2() - min_delay.as_secs_f64().log2()).ceil() as usize + 1;
        // If we were consistently picking the high end of the jitter on max_delay and failing
        // immediately (the worst case), we'd need to keep around attempts for this long to keep
        // delay_for_retry returning max_delay.
        //
        // We forget anything older because they're probably less relevant.
        let max_age = Duration::from_secs_f64(max_delay.as_secs_f64() * (size as f64) * 1.5);

        Self {
            attempts: VecDeque::new(),
            size,
            max_age,
            min_delay,
            max_delay,
        }
    }

    fn next(&mut self) -> Duration {
        self.attempts.push_back(std::time::Instant::now());
        while let Some(attempt) = self.attempts.front() {
            if self.attempts.len() < self.size && attempt.elapsed() < self.max_age {
                break;
            }
            self.attempts.pop_front();
        }

        delay_for_retry(self.attempts.len(), self.min_delay, self.max_delay)
    }
}
