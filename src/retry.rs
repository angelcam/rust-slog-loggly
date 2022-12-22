use tokio_retry::strategy::{FibonacciBackoff, jitter};
use std::future::Future;
use std::{
  iter::{Take, Map},
  time::Duration
};

fn retry_strategy(ms: u64, attempts: usize) -> Take<Map<FibonacciBackoff, fn(Duration) -> Duration>>{
  FibonacciBackoff::from_millis(ms)
    .map(jitter as fn(Duration) -> Duration)
    .take(attempts)
}


pub async fn with_retry<A, F, R, E>(ms: Option<u64>, attempts: Option<usize>, action: A) -> Result<R, E>
  where
    A: FnMut() -> F,
    E: std::fmt::Display,
    F: Future<Output=Result<R, E>>
{
  let attempts = if let Some(attempts) = attempts { attempts } else { 10 };
  let ms = if let Some(ms) = ms { ms } else { 1000 /*1 sec */};

  tokio_retry::Retry::spawn(retry_strategy(ms, attempts), action).await
}
