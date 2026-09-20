use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

use crate::Message;
use crate::services::Services;
use crate::transport::Transport;

enum Decision {
    Consume,
    Forward,
    ConsumeAndForward,
}

const MAX_TTL: u8 = 10;

pub struct ChatService<T: Transport> {
    transport: Arc<T>,
    peers: Arc<AsyncMutex<HashSet<T::Peer>>>,
    rx: AsyncMutex<UnboundedReceiver<(T::Peer, Message)>>,
    own_id: String,
    seen: Mutex<VecDeque<String>>,
}

impl<T: Transport> ChatService<T> {
    pub fn new(
        transport: Arc<T>,
        peers: Arc<AsyncMutex<HashSet<T::Peer>>>,
        rx: UnboundedReceiver<(T::Peer, Message)>,
        own_id: String,
    ) -> Self {
        Self {
            transport,
            peers,
            rx: AsyncMutex::new(rx),
            own_id,
            seen: Mutex::new(VecDeque::new()),
        }
    }

    async fn next_incoming(&self) -> Option<(T::Peer, Message)> {
        self.rx.lock().await.recv().await
    }

    async fn handle(&self, sender_id: &T::Peer, mut message: Message) {
        if let Message::Chat { id, .. } = &message {
            let mut seen = self.seen.lock().unwrap();
            if seen.contains(id) {
                return;
            }
            seen.push_back(id.clone());
            if seen.len() > 1024 {
                seen.pop_front();
            }
        }

        match self.decide(&message) {
            Decision::Consume => self.consume(sender_id, &message).await,
            Decision::Forward => self.forward(Some(sender_id), &mut message).await,
            Decision::ConsumeAndForward => {
                self.consume(sender_id, &message).await;
                self.forward(Some(sender_id), &mut message).await;
            }
        }
    }

    fn decide(&self, message: &Message) -> Decision {
        match message {
            Message::Chat { target, .. } if target.is_empty() => Decision::ConsumeAndForward,
            Message::Chat { target, .. } if target == &self.own_id => Decision::Consume,
            Message::Chat { .. } => Decision::Forward,
            Message::HeartBeat { .. } => Decision::Consume,
        }
    }

    async fn consume(&self, sender_id: &T::Peer, message: &Message) {
        if let Message::Chat { message, .. } = message {
            println!("[{}]: {}", sender_id, message);
        }
    }

    async fn forward(&self, except: Option<&T::Peer>, message: &mut Message) {
        let ttl = match message {
            Message::Chat { ttl, .. } => ttl,
            Message::HeartBeat { .. } => return,
        };
        if *ttl == 0 {
            return;
        }
        *ttl -= 1;

        let mut payload = Vec::new();
        if let Err(e) = ciborium::into_writer(message, &mut payload) {
            eprintln!("Failed to encode message: {}", e);
            return;
        }

        let peers: Vec<T::Peer> = self.peers.lock().await.iter().cloned().collect();
        for peer_id in peers {
            if let Some(except) = except
                && except == &peer_id
            {
                continue;
            }

            let _ = self
                .transport
                .send(&peer_id, &payload)
                .await
                .map_err(|e| eprintln!("Failed sending to peer {}: {}", peer_id, e));
        }
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
                    let mut message = Message::Chat {
                        id: Uuid::new_v4().to_string(),
                        message: local_line.to_string(),
                        target: String::new(),
                        ttl: MAX_TTL,
                    };
                    self.forward(None, &mut message).await;
                }
            }
        }
    }
}
