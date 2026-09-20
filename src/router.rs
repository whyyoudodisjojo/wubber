use std::sync::Arc;

use anyhow::anyhow;
use blew::peripheral::PeripheralRequest;
use blew::{DeviceId, Peripheral};
use futures_util::stream::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

use crate::Message;
use crate::buffers::Buffer;
use crate::buffers::chat::ChatBuffer;

pub struct Router {
    chat_tx: UnboundedSender<(DeviceId, Message)>,
    heartbeat_tx: UnboundedSender<(DeviceId, Message)>,
}

impl Router {
    pub fn new(
        chat_tx: UnboundedSender<(DeviceId, Message)>,
        heartbeat_tx: UnboundedSender<(DeviceId, Message)>,
    ) -> Self {
        Self {
            chat_tx,
            heartbeat_tx,
        }
    }

    pub fn start(self, p: Arc<Peripheral>) -> anyhow::Result<()> {
        let mut req_stream = p.take_requests().ok_or(anyhow!("req stream failed"))?;

        tokio::spawn(async move {
            while let Some(request) = req_stream.next().await {
                if let PeripheralRequest::Write {
                    client_id,
                    char_uuid,
                    value,
                    responder,
                    ..
                } = request
                {
                    if char_uuid == ChatBuffer::UUID
                        && let Ok(message) = ciborium::from_reader::<Message, _>(&value[..])
                    {
                        match message {
                            Message::Chat { .. } => {
                                let _ = self.chat_tx.send((client_id, message));
                            }
                            Message::HeartBeat { .. } => {
                                let _ = self.heartbeat_tx.send((client_id, message));
                            }
                        }
                    }
                    if let Some(responder) = responder {
                        responder.success();
                    }
                }
            }
        });

        Ok(())
    }
}
