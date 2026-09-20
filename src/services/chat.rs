use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use blew::central::{CentralEvent, WriteType};
use blew::{Central, DeviceId};
use futures_util::stream::StreamExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::buffers::chat::ChatBuffer;
use crate::buffers::Buffer;
use crate::services::Services;
use crate::Message;

enum Decision {
    Consume,
    Forward,
    ConsumeAndForward,
}

const MAX_TTL: u8 = 10;

pub struct ChatService {
    central: Arc<Central>,
    peers: Arc<AsyncMutex<HashSet<DeviceId>>>,
    rx: AsyncMutex<UnboundedReceiver<(DeviceId, Message)>>,
    own_id: String,
    seen: Mutex<VecDeque<String>>,
}

impl ChatService {
    pub fn new(
        central: Arc<Central>,
        peers: Arc<AsyncMutex<HashSet<DeviceId>>>,
        rx: UnboundedReceiver<(DeviceId, Message)>,
        own_id: String,
    ) -> Self {
        Self {
            central,
            peers,
            rx: AsyncMutex::new(rx),
            own_id,
            seen: Mutex::new(VecDeque::new()),
        }
    }

    async fn next_incoming(&self) -> Option<(DeviceId, Message)> {
        self.rx.lock().await.recv().await
    }

    async fn handle(&self, sender_id: &DeviceId, mut message: Message) {
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

    async fn consume(&self, sender_id: &DeviceId, message: &Message) {
        if let Message::Chat { message, .. } = message {
            println!("[{}]: {}", sender_id, message);
        }
    }

    async fn forward(&self, except: Option<&DeviceId>, message: &mut Message) {
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

        let peers: Vec<DeviceId> = self.peers.lock().await.iter().cloned().collect();
        for peer_id in peers {
            if let Some(except) = except
                && except == &peer_id
            {
                continue;
            }

            let _ = self
                .central
                .write_characteristic(
                    &peer_id,
                    ChatBuffer::UUID,
                    payload.clone(),
                    WriteType::WithoutResponse,
                )
                .await
                .map_err(|e| eprintln!("Failed sending to peer {}: {}", peer_id, e));
        }
    }
}

impl Services for ChatService {
    async fn run(&self) -> Result<()> {
        let mut events = self.central.events();
        let mut stdin_reader = BufReader::new(tokio::io::stdin()).lines();

        loop {
            tokio::select! {
                Some(event) = events.next() => {
                    if let CentralEvent::CharacteristicNotification { device_id, char_uuid, value } = event
                        && char_uuid == ChatBuffer::UUID
                        && let Ok(message) = ciborium::from_reader::<Message, _>(&value[..])
                    {
                        self.handle(&device_id, message).await;
                    }
                }

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
