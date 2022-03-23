use std::marker::PhantomData;
use std::task::{Context, Poll};

use futures::stream::FuturesOrdered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::DuplexService;

#[derive(Serialize, Debug, Deserialize, Clone)]
struct Req1(String);

#[derive(Serialize, Debug, Deserialize, PartialEq)]
struct Resp1(usize);

#[derive(Serialize, Debug, Deserialize, Clone)]
struct Req2(usize);

#[derive(Serialize, Debug, Deserialize, PartialEq)]
struct Resp2(usize);

impl From<Req1> for Resp1 {
    fn from(v: Req1) -> Self {
        Resp1(v.0.parse().unwrap_or(5555))
    }
}

impl From<Req2> for Resp2 {
    fn from(v: Req2) -> Self {
        Resp2(v.0)
    }
}

/// [`TestService`] will convert a Request to a Response using the [`From`] trait, and return it
/// with a delay.
struct TestService<Request, Response> {
    _req: PhantomData<Request>,
    _res: PhantomData<Response>,
}

impl<Request, Response> Default for TestService<Request, Response> {
    fn default() -> Self {
        TestService {
            _req: Default::default(),
            _res: Default::default(),
        }
    }
}

impl<Request, Response> Service<Request> for TestService<Request, Response>
where
    Request: Into<Response> + Send + 'static,
{
    type Response = Response;
    type Error = ();
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        use rand::prelude::*;

        let mut rng = rand::thread_rng();
        let y: u16 = rng.gen();

        let delay = tokio::time::sleep(std::time::Duration::from_millis((y % 2048) as _));

        Box::pin(async move {
            delay.await;
            Ok(req.into())
        })
    }
}

#[tokio::test]
async fn test_rt() {
    let rpcs_to_run = 1000;

    let ((r1, w1), (r2, w2)) = tokio::net::UnixStream::pair()
        .map(|(a, b)| (a.into_split(), b.into_split()))
        .unwrap();

    let (serv1, mut client1): (DuplexService<Req1, Result<Resp1, ()>, _, Req2>, _) =
        DuplexService::new_pair(TestService::<Req2, Resp2>::default());

    let (serv2, mut client2): (DuplexService<Req2, Result<Resp2, ()>, _, Req1>, _) =
        DuplexService::new_pair(TestService::<Req1, Resp1>::default());

    tokio::spawn(serv1.run(r1, w1));
    tokio::spawn(serv2.run(r2, w2));

    let mut c1 = FuturesOrdered::new();
    let mut c2 = FuturesOrdered::new();

    for i in 0..rpcs_to_run {
        c1.push(client1.call(Req1(i.to_string())));
        c2.push(client2.call(Req2(i)));
    }

    let (r1, r2) = futures::join!(c1.collect::<Vec<_>>(), c2.collect::<Vec<_>>());

    assert_eq!(r1.len(), rpcs_to_run);
    assert_eq!(r2.len(), rpcs_to_run);

    for (i, res) in r1.into_iter().enumerate() {
        assert_eq!(res.unwrap().unwrap(), Resp1(i));
    }

    for (i, res) in r2.into_iter().enumerate() {
        assert_eq!(res.unwrap().unwrap(), Resp2(i));
    }
}
