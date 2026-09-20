use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::Message;
use crate::services::Services;
use crate::transport::Transport;

pub struct ChatService<T: Transport> {
    transport: Arc<T>,
    rx: Mutex<UnboundedReceiver<(T::Peer, Message)>>,
    own_id: String,
}

impl<T: Transport> ChatService<T> {
    pub fn new(
        transport: Arc<T>,
        rx: UnboundedReceiver<(T::Peer, Message)>,
        own_id: String,
    ) -> Self {
        Self {
            transport,
            rx: Mutex::new(rx),
            own_id,
        }
    }

    async fn next_incoming(&self) -> Option<(T::Peer, Message)> {
        self.rx.lock().await.recv().await
    }

    async fn handle(&self, sender_id: &T::Peer, message: Message) {
        if let Message::Chat { message, target } = message {
            if target.is_empty() || target == self.own_id {
                println!("[{}]: {}", sender_id, message);
            }
        }
    }

    async fn send_broadcast(&self, message: &Message) {
        let mut payload = Vec::new();
        let _ = ciborium::into_writer(message, &mut payload)
            .map_err(|e| eprintln!("Failed to encode message: {e}"));
        let _ = self
            .transport
            .broadcast(&payload)
            .await
            .map_err(|e| eprintln!("Failed to broadcast message: {e}"));
    }
}

impl<T: Transport> Services for ChatService<T> {
    async fn run(&self) -> Result<()> {
        let mut stdin_reader = BufReader::new(tokio::io::stdin()).lines();

        loop {
            tokio::select! {
                Some((remote_sender_id, message)) = self.next_incoming() => {
                    self.handle(&remote_sender_id, message).await;
                }

                Ok(Some(local_line)) = stdin_reader.next_line() => {
                    println!("[You]: {}", local_line);
                    let message = Message::Chat {
                        message: local_line,
                        target: String::new(),
                    };
                    self.send_broadcast(&message).await;
                }
            }
        }
    }
}
