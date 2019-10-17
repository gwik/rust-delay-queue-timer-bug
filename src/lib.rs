extern crate tokio;
extern crate futures;

use futures::prelude::*;
use futures::sync::BiLock;
use std::time::Instant;
use tokio::timer::*;

struct DelayWrapper {
    queue: DelayQueue<String>,
}

impl DelayWrapper {
    fn split(self) -> (DelaySink, DelayStream) {
        let (sink_lock, stream_lock) = BiLock::new(self);
        (DelaySink(sink_lock), DelayStream(stream_lock))
    }
}

struct DelayStream(BiLock<DelayWrapper>);

impl Stream for DelayStream {
    type Item = String;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<String>, Error> {
        let start = Instant::now();

        loop {
            println!("POLL {:?}", Instant::now().duration_since(start));
            match self.0.poll_lock() {
                Async::Ready(mut inner) => {

                    println!("READY TO POLL {:?}", Instant::now().duration_since(start));

                    match inner.queue.poll() {
                        Ok(Async::Ready(Some(expired))) => {
                            let s = expired.into_inner();
                            println!("FIRED {:}", s);
                            return Ok(Async::Ready(Some(s)));
                        },
                        Ok(Async::Ready(None)) => (),
                        Ok(Async::NotReady) => (),
                        Err(e) => return Err(e),
                    }
                },
                Async::NotReady => (),
            }
        }
    }
}

struct DelaySink(BiLock<DelayWrapper>);

impl Sink for DelaySink {
    type SinkItem = (String, Instant);
    type SinkError = ();

    fn start_send(&mut self, item: (String, Instant)) -> StartSend<(String, Instant), ()> {
        match self.0.poll_lock() {
            Async::Ready(mut inner) => {
                println!("INSERT: {:} {:?}", item.0, item.1);
                inner.queue.insert_at(item.0, item.1);
                Ok(AsyncSink::Ready)
            },
            Async::NotReady => Ok(AsyncSink::NotReady(item)),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod tests {

    use tokio::prelude::*;
    use tokio::timer::Delay;


    #[test]
    fn it_works() {
        use std::time::{Duration, Instant};

        let when = Instant::now() + Duration::from_millis(100);

        tokio::run({
            Delay::new(when)
                .map_err(|e| panic!("timer failed; err={:?}", e))
                .and_then(|_| {
                    println!("Hello world!");
                    Ok(())
                })
        })
    }

    #[test]
    fn timers_are_stuck() {

        use futures::future::lazy;
        use tokio::timer::DelayQueue;
        use super::*;

        tokio::run(lazy(||{
            use std::time::Duration;

            let w = DelayWrapper{
                queue: DelayQueue::new(),
            };

            let (sink, stream) = w.split();

            let samples = vec!(
                (
                    "before now will fire".to_string(),
                    Instant::now() - Duration::from_millis(500),
                ),
                (
                    "after now will never fire".to_string(),
                    Instant::now() + Duration::from_millis(1000),                    
                ),
                (
                    "before now will fire again".to_string(),
                    Instant::now() - Duration::from_millis(500),                    
                ),
            );

            let n = samples.len();

            tokio::spawn(lazy(|| {
                let input = stream::iter_ok(samples);
                sink.send_all(input).map(|_| ()).map_err(|err| panic!(err))
            }));

            stream
                .timeout(Duration::from_secs(5))
                .take(n as u64)
                .map(|out| println!("out: {:}", out))
                .map_err(|err| panic!(err))
                .fold((), |_, _| Ok(()))
        }));
    }

}
