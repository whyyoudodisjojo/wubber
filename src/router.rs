use std::sync::Arc;

use anyhow::Result;
use futures_util::stream::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

use crate::Message;
use crate::transport::Transport;

pub struct Router<T: Transport> {
    chat_tx: UnboundedSender<(T::Peer, Message)>,
    heartbeat_tx: UnboundedSender<(T::Peer, Message)>,
}

impl<T: Transport> Router<T> {
    pub fn new(
        chat_tx: UnboundedSender<(T::Peer, Message)>,
        heartbeat_tx: UnboundedSender<(T::Peer, Message)>,
    ) -> Self {
        Self {
            chat_tx,
            heartbeat_tx,
        }
    }

    pub fn start(self, transport: Arc<T>) -> Result<()> {
        tokio::spawn(async move {
            let mut incoming = transport.incoming();

            while let Some((peer, data)) = incoming.next().await {
                if let Ok(message) = ciborium::from_reader::<Message, _>(&data[..]) {
                    match message {
                        Message::Chat { .. } => {
                            let _ = self.chat_tx.send((peer, message));
                        }
                        Message::HeartBeat { .. } => {
                            let _ = self.heartbeat_tx.send((peer, message));
                        }
                    }
                }
            }
        });

        Ok(())
    }
}
