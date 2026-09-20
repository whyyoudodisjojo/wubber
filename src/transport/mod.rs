pub mod ble;

use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Result;
use futures_util::Stream;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

pub type Incoming<Id> = (Id, Vec<u8>);

pub trait Transport: Send + Sync + Sized + 'static {
    type Config: Send;

    type Peer: Clone + Eq + Hash + Display + Send + Sync + 'static;
    type Incoming: Stream<Item = Incoming<Self::Peer>> + Send + Unpin + 'static;
    type Discovered: Stream<Item = Self::Peer> + Send + Unpin + 'static;

    fn new(config: Self::Config) -> impl Future<Output = Result<Self>> + Send;

    fn advertise(&self) -> impl Future<Output = Result<()>> + Send;
    fn scan(&self) -> impl Future<Output = Result<()>> + Send;
    fn discovered(&self) -> Self::Discovered;
    fn connect(&self, peer: &Self::Peer) -> impl Future<Output = Result<()>> + Send;
    fn subscribe(&self, peer: &Self::Peer) -> impl Future<Output = Result<()>> + Send;
    fn send(&self, peer: &Self::Peer, data: &[u8]) -> impl Future<Output = Result<()>> + Send;
    fn disconnect(&self, peer: &Self::Peer) -> impl Future<Output = Result<()>> + Send;
    fn incoming(&self) -> Self::Incoming;
}

pub struct BroadcastEventStream<T: Clone + Send + 'static>(BroadcastStream<T>);

impl<T: Clone + Send + 'static> From<broadcast::Receiver<T>> for BroadcastEventStream<T> {
    fn from(rx: broadcast::Receiver<T>) -> Self {
        Self(BroadcastStream::new(rx))
    }
}

impl<T: Clone + Send + 'static> Stream for BroadcastEventStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.0).poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(item)),
                Poll::Ready(Some(Err(_))) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T: Clone + Send + 'static> Unpin for BroadcastEventStream<T> {}
