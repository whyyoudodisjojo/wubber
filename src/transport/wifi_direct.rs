use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use futures_util::stream::StreamExt;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use zbus::proxy;
use zbus::zvariant::{ObjectPath, OwnedObjectPath, OwnedValue, Str};

use super::{BroadcastEventStream, Incoming, Transport};

pub struct WifiDirectConfig {
    pub interface: String,
    pub name: String,
    pub port: u16,
}

#[proxy(
    default_service = "fi.w1.wpa_supplicant1",
    default_path = "/fi/w1/wpa_supplicant1",
    interface = "fi.w1.wpa_supplicant1"
)]
trait Supplicant {
    fn get_interface(&self, ifname: &str) -> zbus::Result<OwnedObjectPath>;
    fn create_interface(&self, args: HashMap<String, OwnedValue>) -> zbus::Result<OwnedObjectPath>;
}

#[proxy(
    default_service = "fi.w1.wpa_supplicant1",
    interface = "fi.w1.wpa_supplicant1.Interface.P2PDevice"
)]
trait P2pDevice {
    fn find(&self, args: HashMap<String, OwnedValue>) -> zbus::Result<()>;
    fn stop_find(&self) -> zbus::Result<()>;
    fn connect(&self, args: HashMap<String, OwnedValue>) -> zbus::Result<String>;
    fn invite(&self, args: HashMap<String, OwnedValue>) -> zbus::Result<()>;
    fn remove_client(&self, args: HashMap<String, OwnedValue>) -> zbus::Result<()>;

    #[zbus(property)]
    fn set_p2p_device_config(&self, config: HashMap<String, OwnedValue>) -> zbus::Result<()>;

    #[zbus(signal)]
    fn device_found(&self, path: OwnedObjectPath) -> zbus::Result<()>;
}

pub struct WifiDirectTransport {
    connection: zbus::Connection,
    iface: String,
    socket: Arc<UdpSocket>,
    port: u16,
    incoming_tx: broadcast::Sender<Incoming<String>>,
    discovered_tx: broadcast::Sender<String>,
}

impl WifiDirectTransport {
    async fn p2p(&self) -> Result<P2pDeviceProxy<'_>> {
        let builder = P2pDeviceProxy::builder(&self.connection)
            .path(self.iface.as_str())
            .map_err(|e| anyhow!("invalid interface path {}: {e}", self.iface))?;
        Ok(builder.build().await?)
    }

    fn peer_object_path(&self, mac: &str) -> String {
        format!("{}/Peers/{mac}", self.iface)
    }
}

impl Transport for WifiDirectTransport {
    type Config = WifiDirectConfig;
    type Peer = String;
    type Incoming = BroadcastEventStream<Incoming<String>>;
    type Discovered = BroadcastEventStream<String>;

    async fn new(config: Self::Config) -> Result<Self> {
        let connection = zbus::conn::Builder::system()?.build().await?;

        let supplicant = SupplicantProxy::new(&connection).await?;
        let iface = match supplicant.get_interface(&config.interface).await {
            Ok(path) => path.to_string(),
            Err(_) => {
                let mut args = HashMap::new();
                args.insert(
                    String::from("Ifname"),
                    OwnedValue::from(Str::from(config.interface.as_str())),
                );
                args.insert(
                    String::from("Driver"),
                    OwnedValue::from(Str::from("nl80211")),
                );
                supplicant.create_interface(args).await?.to_string()
            }
        };

        let p2p = P2pDeviceProxy::builder(&connection)
            .path(iface.as_str())?
            .build()
            .await?;
        let device = HashMap::from([(
            String::from("DeviceName"),
            OwnedValue::from(Str::from(config.name)),
        )]);
        let _ = p2p.set_p2p_device_config(device).await;

        let socket =
            Arc::new(UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.port)).await?);
        socket
            .set_broadcast(true)
            .map_err(|e| anyhow!("wifi-direct: enable broadcast: {e}"))?;

        let (incoming_tx, _) = broadcast::channel(1024);
        let (discovered_tx, _) = broadcast::channel(1024);

        let dp_connection = connection.clone();
        let dp_iface = iface.clone();
        let dp_discovered = discovered_tx.clone();
        tokio::spawn(async move {
            let p2p = P2pDeviceProxy::builder(&dp_connection).path(dp_iface.as_str());
            let Ok(builder) = p2p else { return };
            let Ok(p2p) = builder.build().await else {
                return;
            };
            let Ok(mut found) = p2p.receive_device_found().await else {
                return;
            };

            while let Some(message) = found.next().await {
                let Ok(args) = message.args() else { continue };
                let Some(mac) = args
                    .path
                    .as_str()
                    .rsplit('/')
                    .next()
                    .filter(|s| !s.is_empty())
                else {
                    continue;
                };
                if dp_discovered.send(mac.to_owned()).is_err() {
                    break;
                }
            }
        });

        let udp_socket = socket.clone();
        let up_incoming = incoming_tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match udp_socket.recv_from(&mut buf).await {
                    Ok((n, addr)) => {
                        let peer = match addr.ip() {
                            IpAddr::V4(ip) => {
                                let mut mac = None;
                                if let Ok(table) = std::fs::read_to_string("/proc/net/arp") {
                                    for line in table.lines().skip(1) {
                                        let mut columns = line.split_whitespace();
                                        let Some(addr) = columns.next() else { continue };
                                        let Some(_hw) = columns.next() else { continue };
                                        let Some(_flags) = columns.next() else {
                                            continue;
                                        };
                                        let Some(hw) = columns.next() else { continue };
                                        if let Ok(addr) = addr.parse::<Ipv4Addr>()
                                            && addr == ip
                                            && hw != "00:00:00:00:00:00"
                                        {
                                            mac = Some(hw.to_string());
                                            break;
                                        }
                                    }
                                }
                                mac
                            }
                            IpAddr::V6(_) => None,
                        }
                        .unwrap_or_else(|| addr.ip().to_string());

                        if up_incoming.send((peer, buf[..n].to_vec())).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("wifi-direct: udp recv failed: {e}");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            connection,
            iface,
            socket,
            port: config.port,
            incoming_tx,
            discovered_tx,
        })
    }

    async fn advertise(&self) -> Result<()> {
        Ok(())
    }

    async fn scan(&self) -> Result<()> {
        self.p2p().await?.find(HashMap::new()).await?;
        Ok(())
    }

    fn discovered(&self) -> Self::Discovered {
        self.discovered_tx.subscribe().into()
    }

    fn incoming(&self) -> Self::Incoming {
        self.incoming_tx.subscribe().into()
    }

    async fn connect(&self, peer: &Self::Peer) -> Result<()> {
        let mut args = HashMap::new();
        args.insert(
            String::from("peer"),
            OwnedValue::from(ObjectPath::try_from(self.peer_object_path(peer))?),
        );
        args.insert(
            String::from("wps_method"),
            OwnedValue::from(Str::from("pbc")),
        );
        self.p2p().await?.connect(args).await?;
        Ok(())
    }

    async fn subscribe(&self, _peer: &Self::Peer) -> Result<()> {
        Ok(())
    }

    async fn send(&self, _peer: &Self::Peer, data: &[u8]) -> Result<()> {
        self.socket
            .send_to(data, SocketAddrV4::new(Ipv4Addr::BROADCAST, self.port))
            .await?;
        Ok(())
    }

    async fn disconnect(&self, peer: &Self::Peer) -> Result<()> {
        let mut args = HashMap::new();
        args.insert(
            String::from("peer"),
            OwnedValue::from(ObjectPath::try_from(self.peer_object_path(peer))?),
        );
        let _ = self.p2p().await?.remove_client(args).await;
        Ok(())
    }
}
