//! A [`tower::Service`] that implements a server and a client simultaneously over a
//! bi-directional channel. As a server it is able to process RPC calls from a remote client,
//! and as a client it is capable of making RPC calls into a remote server. It is very
//! convinient in a system that requires asynchronous communication in both directions.
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::pending;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{Sink, SinkExt, StreamExt, TryStream, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tower::Service;

mod codec;
mod serialize;
#[cfg(test)]
mod test;

/// A wrapper for an RPC request or response. The wrapper includes a tag to demultiplex responses
/// and match them to the correct requests.
#[derive(Debug)]
pub enum DuplexValue<Request, Response> {
    Request(u8, Request),
    Response(u8, Response),
}

/// A [`tower::Service`] that implements a server and a client simultaneously over a bi-directional
/// channel. As a server it is able to process RPC calls from a remote client, and as a client it is
/// capable of making RPC calls into a remote server. It is very convinient in a system that
/// requires asynchronous communication in both directions.
pub struct DuplexService<Request, Response, S: Service<ServiceRequest>, ServiceRequest> {
    calls: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Response>)>,
    service: S,
    load: Arc<AtomicUsize>,
    _p: PhantomData<ServiceRequest>,
}

/// A client side handle to [`DuplexService`], used for RPC calls to the remote server.
#[derive(Clone)]
pub struct DuplexClient<Request, Response> {
    sender: mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>,
    load: Arc<AtomicUsize>,
}

pub enum DuplexError<E1, E2> {
    RemoteHangUp,
    RemoteError(E1),
    SendError(E2),
}

/// Tagger generates unique tag values for RPC calls, required to match requests to responses when
/// multiplexing requests. The used tags are backed by a 128 bitarray, and therefore the maximal
/// amount of tags generated is 128. Which is also the maximum we set for concurrent RPC calls in
/// flight.
struct Tagger {
    bitmask: u128,
}

impl Tagger {
    fn new() -> Self {
        Tagger { bitmask: 0 }
    }

    /// Get the smallest available tag value
    fn get_tag(&mut self) -> Option<u8> {
        // Simply look for the next unset bit
        let r = self.bitmask.trailing_ones() as u8;
        self.bitmask |= 1u128.checked_shl(r as _)?;
        Some(r)
    }

    /// Return the tag after we finished using it. It is very impotant to release tags, otherwise
    /// the system will run out of tags very quickly.
    fn release_tag(&mut self, tag: u8) {
        self.bitmask &= !(1 << tag);
    }

    /// Check if tagger can't allocate any more tags
    fn full(&self) -> bool {
        self.bitmask == u128::MAX
    }
}

/// Feed the queued packets into our sink, then flush. This is implemented as a standalone function
/// so we can use it in a select loop with FuturesOrdered (that implement StreamExt and therefore
/// cancellation safe). Once done the function returns the owned sink, so more writes can be
/// scheduled on it.
async fn do_send<I, S: Sink<I> + Unpin>(items: Vec<I>, sender: Option<S>) -> Result<S, S::Error> {
    let mut sender = match sender {
        Some(sender) => sender,
        None => pending().await,
    };

    for item in items {
        sender.feed(item).await?;
    }

    sender.flush().await?;

    Ok(sender)
}

impl<Request, Response, S: Service<ServiceRequest>, ServiceRequest>
    DuplexService<Request, Response, S, ServiceRequest>
{
    // This is a nice workaround for initiating an array with a non Copy type
    // https://github.com/rust-lang/rust/issues/44796#issuecomment-967747810
    const INIT_ARR: Option<oneshot::Sender<Response>> = None;

    /// Create a new server instance, with an associated client handle. The server stops if all the
    /// client handles are dropped. To start the server use the [`run`] or [`run_with`] methods.
    ///
    /// # Example
    ///
    /// ```
    /// use core::task::{Context, Poll};
    ///
    /// use tower_duplex::DuplexService;
    ///
    /// /// A Service that converts requests to lower or upper case
    /// enum ChangeCase {
    ///     ToLower,
    ///     ToUpper,
    /// }
    ///
    /// impl tower::Service<String> for ChangeCase {
    ///     type Response = String;
    ///     type Error = ();
    ///     type Future = std::pin::Pin<
    ///         Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    ///     >;
    ///
    ///     fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
    ///         Poll::Ready(Ok(()))
    ///     }
    ///
    ///     fn call(&mut self, req: String) -> Self::Future {
    ///         let to_upper = matches!(self, ChangeCase::ToUpper);
    ///         Box::pin(async move {
    ///             if to_upper {
    ///                 Ok(req.to_uppercase())
    ///             } else {
    ///                 Ok(req.to_lowercase())
    ///             }
    ///         })
    ///     }
    /// }
    ///
    /// let (server, client): (DuplexService<String, String, _, _>, _) =
    ///     DuplexService::new_pair(ChangeCase::ToUpper);
    /// ```
    pub fn new_pair(service: S) -> (Self, DuplexClient<Request, Response>) {
        let load = Arc::new(AtomicUsize::new(0));
        let (calls_sender, calls) = mpsc::unbounded_channel();
        (
            DuplexService {
                calls,
                service,
                load: load.clone(),
                _p: Default::default(),
            },
            DuplexClient {
                sender: calls_sender,
                load,
            },
        )
    }

    /// Run the server loop with the provided [`TryStream`] and [`Sink`]. The server loop
    /// serves remote RPC calls, and handles local RPC calls from client handles.
    pub async fn run_with<Rcv, Snd, RcvErr, SndErr>(
        mut self,
        mut receiver: Rcv,
        sender: Snd,
    ) -> Result<(), DuplexError<RcvErr, SndErr>>
    where
        Rcv: TryStream<Ok = DuplexValue<ServiceRequest, Response>, Error = RcvErr> + Unpin,
        Snd: Sink<
                DuplexValue<Request, <<S as Service<ServiceRequest>>::Future as Future>::Output>,
                Error = SndErr,
            > + Unpin,
    {
        // A list of pending calls to the inner service
        let mut pending_calls = FuturesUnordered::new();
        // A list of pening RPC return channels
        let mut pending_rpcs: [Option<oneshot::Sender<Response>>; 128] = [Self::INIT_ARR; 128];

        let mut tagger = Tagger::new();

        let mut sender = Some(sender);

        // Send items that will be sent in the next send op
        let mut pending_send = Vec::new();
        // Send items that didn't get a tag and will be sent once there is room in the send queue
        let mut sending_queue = VecDeque::new();
        let mut send_fut = FuturesOrdered::new();

        loop {
            while !sending_queue.is_empty() && !tagger.full() {
                // Move items to the from queue to pending
                let tag = tagger.get_tag().expect("Tagger not full");
                let (request, result_sender) = sending_queue.pop_front().expect("Queue not empty");
                tracing::trace!("New RPC call {tag}");
                pending_send.push(DuplexValue::Request(tag, request));
                pending_rpcs[tag as usize] = Some(result_sender);
            }

            if sender.is_some() && !pending_send.is_empty() {
                // We got something to send out, and we are not already sending anything
                tracing::trace!("Flushing send buffer");
                let to_send = pending_send.split_off(0);
                send_fut.push(do_send(to_send, sender.take()));
            }

            let DuplexService {
                service,
                calls,
                load,
                ..
            } = &mut self;
            tokio::select! {
                response = receiver.try_next() => {
                    match response {
                        Ok(Some(DuplexValue::Request(tag, req))) => {
                            tracing::trace!("New request {tag}");
                            // This is a request from a remote client (or server/client) to perform an RPC.
                            // Generate the future for the RPC and push it along with the tag to the list of executing calls.
                            let fut = service.call(req);
                            pending_calls.push(async move { (fut.await, tag) });
                        }
                        Ok(Some(DuplexValue::Response(tag, res))) => {
                            tracing::trace!("Response for {tag}");
                            // This is a response from the remote server (or server/client) to an RPC we initiated.
                            // Match the tag of the response to a channel on which to send the response.
                            match pending_rpcs.get_mut(tag as usize).and_then(Option::take) {
                                None => tracing::error!("No matching request for response {tag}"),
                                Some(chan) => if chan.send(res).is_err() {
                                    tracing::debug!("Channel for response {tag} went away");
                                },
                            }
                            tagger.release_tag(tag);
                            load.fetch_sub(1, SeqCst);
                        }
                        Err(err) => return Err(DuplexError::RemoteError(err)),
                        Ok(None) => return Err(DuplexError::RemoteHangUp)
                    }
                }
                Some((request, result_sender)) = calls.recv() => {
                    // This is a request from our own client handle to perform an RPC call.
                    if let Some(tag) = tagger.get_tag() {
                        tracing::trace!("New RPC call {tag}");
                        pending_send.push(DuplexValue::Request(tag, request));
                        pending_rpcs[tag as usize] = Some(result_sender);
                    } else {
                        tracing::trace!("Queued RPC call");
                        sending_queue.push_back((request, result_sender));
                    }
                    load.fetch_add(1, SeqCst);
                }
                Some((result, tag)) = pending_calls.next() => {
                    // One of the executed calls is finished, send it back to the remote client (or server/client).
                    tracing::trace!("Call {tag} finished");
                    pending_send.push(DuplexValue::Response(tag, result));
                }
                Some(send_result) = send_fut.next() => {
                    match send_result {
                        Ok(enc) => {
                            // Return the codec to its place, so we can issue the next send operation
                            sender.replace(enc);
                        },
                        Err(err) => return Err(DuplexError::SendError(err)),
                    }
                }
            }
        }
    }
}

impl<Request, Response, S: Service<ServiceRequest>, ServiceRequest>
    DuplexService<Request, Response, S, ServiceRequest>
where
    for<'de> ServiceRequest: Serialize + Deserialize<'de>,
    for<'de> <<S as Service<ServiceRequest>>::Future as Future>::Output:
        Serialize + Deserialize<'de>,
    for<'de> Request: Serialize + Deserialize<'de>,
    for<'de> Response: Serialize + Deserialize<'de>,
{
    /// Run the server loop with the provided [`AsyncRead`] and [`AsyncWrite`]. The server loop
    /// serves remote RPC calls, and handles local RPC calls from client handles.
    ///
    /// # Example
    ///
    /// ```
    /// use core::task::{Context, Poll};
    ///
    /// use tokio::sync::mpsc;
    /// use tower::Service;
    /// use tower_duplex::DuplexService;
    ///
    /// /// A Service that converts requests to lower or upper case
    /// enum ChangeCase {
    ///     ToLower,
    ///     ToUpper,
    /// }
    ///
    /// impl tower::Service<String> for ChangeCase {
    ///     type Response = String;
    ///     type Error = ();
    ///     type Future = std::pin::Pin<
    ///         Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    ///     >;
    ///
    ///     fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
    ///         Poll::Ready(Ok(()))
    ///     }
    ///
    ///     fn call(&mut self, req: String) -> Self::Future {
    ///         let to_upper = matches!(self, ChangeCase::ToUpper);
    ///         Box::pin(async move {
    ///             if to_upper {
    ///                 Ok(req.to_uppercase())
    ///             } else {
    ///                 Ok(req.to_lowercase())
    ///             }
    ///         })
    ///     }
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     // `server1` handles serves requests from `server2` and converts strings to upper case.
    ///     // It also forwards requests from `client1` to `server2`.
    ///     let (server1, mut client1): (DuplexService<String, Result<String, ()>, _, _>, _) =
    ///         DuplexService::new_pair(ChangeCase::ToUpper);
    ///     // `server2` handles serves requests from `server1` and converts strings to lower case.
    ///     // It also forwards requests from `client2` to `server1`.
    ///     let (server2, mut client2): (DuplexService<String, Result<String, ()>, _, _>, _) =
    ///         DuplexService::new_pair(ChangeCase::ToLower);
    ///
    ///     let ((r1, w1), (r2, w2)) = tokio::net::UnixStream::pair()
    ///         .map(|(a, b)| (a.into_split(), b.into_split()))
    ///         .unwrap();
    ///
    ///     tokio::spawn(server1.run(r1, w1));
    ///     tokio::spawn(server2.run(r2, w2));
    ///
    ///     assert_eq!(
    ///         client2.call("String".to_string()).await.unwrap().unwrap(),
    ///         "STRING"
    ///     );
    ///
    ///     assert_eq!(
    ///         client1.call("String".to_string()).await.unwrap().unwrap(),
    ///         "string"
    ///     );
    /// }
    /// ```
    pub async fn run<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        self,
        reader: R,
        writer: W,
    ) -> Result<(), DuplexError<std::io::Error, std::io::Error>> {
        let decoder: codec::FrameCodec<DuplexValue<ServiceRequest, Response>> = Default::default();
        let encoder: codec::FrameCodec<
            DuplexValue<Request, <<S as Service<ServiceRequest>>::Future as Future>::Output>,
        > = Default::default();

        let frame_reader = tokio_util::codec::FramedRead::new(reader, decoder);
        let frame_writer = tokio_util::codec::FramedWrite::new(writer, encoder);

        self.run_with(frame_reader, frame_writer).await
    }
}

impl<Request, Response> Service<Request> for DuplexClient<Request, Response> {
    type Response = Response;

    type Error = oneshot::error::RecvError;

    type Future = oneshot::Receiver<Response>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let (reseponse_send, response_recv) = oneshot::channel();
        // We ignore the send error here, because if send fails it just means the service has
        // stopped, in which case our oneshot will immediately get dropped and an error returned
        // from the future
        let _ = self.sender.send((req, reseponse_send));
        response_recv
    }
}

impl<Request, Response> tower::load::Load for DuplexClient<Request, Response> {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        self.load.load(SeqCst)
    }
}

impl<Request, Response> DuplexClient<Request, Response> {
    pub async fn do_call(&self, req: Request) -> Result<Response, oneshot::error::RecvError> {
        let (reseponse_send, response_recv) = oneshot::channel();
        // We ignore the send error here, because if send fails it just means the service has
        // stopped, in which case our oneshot will immediately get dropped and an error returned
        // from the future
        let _ = self.sender.send((req, reseponse_send));
        response_recv.await
    }
}
