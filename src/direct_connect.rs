use actor_helper::{Action, Handle, Receiver, act, act_ok};
use anyhow::Result;
use iroh::{Endpoint, EndpointId, endpoint::Connection, protocol::ProtocolHandler};
use n0_watcher::Watchable;
use noq::VarInt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace};

use crate::{
    Router, RouterIp,
    connection::{Conn, InnerConnState},
    local_networking::Ipv4Pkg,
};

const MAX_RECONNECT_ATTEMPTS: usize = 50;

#[derive(Debug, Clone)]
pub struct Direct {
    api: Handle<DirectActor, anyhow::Error>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerState {
    NoConnection,
    Connecting,
    Connected(Conn),
}

#[derive(Debug, Clone)]
pub struct ConnGen {
    accept_conn: Arc<Mutex<PeerState>>,
    open_conn: Arc<Mutex<PeerState>>,
    attempts: Watchable<usize>,
}

impl Default for ConnGen {
    fn default() -> Self {
        Self {
            accept_conn: Arc::new(Mutex::new(PeerState::NoConnection)),
            open_conn: Arc::new(Mutex::new(PeerState::NoConnection)),
            attempts: Watchable::new(0),
        }
    }
}

#[derive(Debug)]
struct DirectActor {
    peers: HashMap<EndpointId, ConnGen>,
    endpoint: iroh::endpoint::Endpoint,
    direct_connect_tx: tokio::sync::mpsc::Sender<DirectMessage>,
    router: Option<Router>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum DirectMessage {
    IpPacket(Ipv4Pkg),
    IDontLikeWarnings,
}

impl Direct {
    pub const ALPN: &[u8] = b"/iroh/lan-direct/1.0.2";
    pub fn new(
        endpoint: iroh::endpoint::Endpoint,
        direct_connect_tx: tokio::sync::mpsc::Sender<DirectMessage>,
    ) -> Self {
        let (api, _) = Handle::spawn_with(
            DirectActor {
                peers: HashMap::new(),
                endpoint,
                direct_connect_tx,
                router: None,
            },
            |mut actor, rx| async move { actor.run(rx).await },
        );
        Self { api }
    }

    pub async fn handle_connection(&self, conn: Connection) -> Result<()> {
        self.api
            .call(act!(actor => actor.handle_connection(conn)))
            .await
    }

    pub async fn route_packet(&self, to: EndpointId, pkg: DirectMessage) -> Result<()> {
        self.api
            .call(act!(actor => actor.route_packet(to, pkg)))
            .await
    }

    pub async fn ensure_connection(&self, to: EndpointId) -> Result<()> {
        self.api.call(act!(actor => actor.ensure_peer(to))).await
    }

    pub async fn set_router(&self, router: Router) -> Result<()> {
        self.api
            .call(act_ok!(actor => async move { actor.set_router(router) }))
            .await
    }

    pub async fn get_endpoint(&self) -> iroh::endpoint::Endpoint {
        self.api
            .call(act!(actor => actor.get_endpoint()))
            .await
            .unwrap()
    }

    pub async fn get_peer_state(&self, endpoint_id: EndpointId) -> Result<InnerConnState> {
        self.api
            .call(act!(actor => actor.get_peer_state(endpoint_id)))
            .await
    }

    pub async fn close(&self) -> Result<()> {
        self.api.call(act!(actor => actor.close())).await
    }
}

impl DirectActor {
    async fn run(&mut self, rx: Receiver<Action<DirectActor>>) -> Result<(), anyhow::Error> {
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut reconnect_interval = tokio::time::interval(std::time::Duration::from_millis(500));
        reconnect_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        debug!("DirectActor run loop started");
        loop {
            tokio::select! {
                Ok(action) = rx.recv_async() => {
                    action(self).await;
                }
                _ = cleanup_interval.tick() => {
                    //self.prune_closed_connections().await;
                }
                _ = reconnect_interval.tick() => {
                    self.connection_driver().await;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl-C received, shutting down DirectActor");
                    break
                }
            }
        }
        Ok(())
    }
}

impl DirectActor {
    /*
    async fn prune_closed_connections(&mut self) {
        let mut to_remove = Vec::new();
        for (id, conn) in &self.peers {
            if conn.get_state().await == crate::connection::ConnState::Closed {
                to_remove.push(*id);
            }
        }
        for id in to_remove {
            debug!("Removing closed connection to {}", id);
            self.peers.remove(&id);
        }
    } */

    async fn connection_driver(&mut self) {
        for (id, peer) in self.peers.clone() {
            // check reserve connections for alive
            {
                let mut open_guard = peer.open_conn.lock().await;
                if let PeerState::Connected(conn) = &*open_guard
                    && !conn.is_alive().await
                {
                    debug!("Open connection to {} is no longer alive, dropping", id);
                    conn.drop().await;
                    *open_guard = PeerState::NoConnection;
                }

                if *open_guard == PeerState::NoConnection
                    && peer.attempts.get() <= MAX_RECONNECT_ATTEMPTS
                {
                    debug!(
                        "Peer {} has no open connection and {} attempts, will try to reconnect",
                        id,
                        peer.attempts.get()
                    );
                    drop(open_guard);
                    Self::open_new_connection(
                        id,
                        peer.clone(),
                        self.endpoint.clone(),
                        self.direct_connect_tx.clone(),
                    )
                    .await;
                }
            }

            {
                let mut accept_guard = peer.accept_conn.lock().await;
                if let PeerState::Connected(conn) = &*accept_guard
                    && !conn.is_alive().await
                {
                    debug!("Accept connection to {} is no longer alive, dropping", id);
                    conn.drop().await;
                    *accept_guard = PeerState::NoConnection;
                }
            }
        }
    }

    async fn open_new_connection(
        peer_id: EndpointId,
        peer: ConnGen,
        endpoint: Endpoint,
        direct_connect_tx: tokio::sync::mpsc::Sender<DirectMessage>,
    ) {
        let mut guard = peer.open_conn.lock().await;
        *guard = PeerState::Connecting;
        drop(guard);

        // Try open new connection
        tokio::spawn(async move {
            info!("Attempting to open new connection to {}", peer_id);
            match Conn::open_connection(endpoint.clone(), peer_id, direct_connect_tx).await {
                Ok(new_conn) => {
                    debug!("Successfully established connection to {}", peer_id);
                    let mut guard = peer.open_conn.lock().await;
                    if *guard != PeerState::Connecting {
                        debug!(
                            "Connection to {} is no longer in Connecting state, dropping new connection",
                            peer_id
                        );
                        new_conn.drop().await;
                        return;
                    }
                    if new_conn.is_alive().await {
                        *guard = PeerState::Connected(new_conn.clone());
                        info!("New connection to {} is now open", peer_id);
                    } else {
                        debug!(
                            "New connection to {} is not alive after establishment, dropping",
                            peer_id
                        );
                        new_conn.drop().await;
                        *guard = PeerState::NoConnection;
                    }
                }
                Err(err) => {
                    error!("Failed to establish connection to {}: {}", peer_id, err);
                    let mut guard = peer.open_conn.lock().await;
                    *guard = PeerState::NoConnection;
                }
            }
        });
    }

    async fn handle_connection(&mut self, conn: iroh::endpoint::Connection) -> Result<()> {
        info!("Handling new connection from {}", conn.remote_id());
        let router = self.router.clone();
        let peer = self.peers.entry(conn.remote_id()).or_default().clone();
        let direct_connect_tx = self.direct_connect_tx.clone();

        {
            let mut guard = peer.accept_conn.lock().await;
            if let PeerState::Connected(existing_conn) = &*guard {
                existing_conn.drop().await;
            }

            *guard = PeerState::Connecting;
        }

        info!("New direct connection from {:?}", conn.remote_id());
        let remote_id = conn.remote_id();

        // logging
        if let Some(router) = &router {
            if !matches!(router.get_ip_state().await, Ok(RouterIp::AssignedIp(_))) {
                info!(
                    "Accepting connection from {} before local IP assignment",
                    remote_id
                );
            }

            if router.get_ip_from_endpoint_id(remote_id).await.is_err() {
                info!(
                    "Accepting connection from {} before remote IP assignment",
                    remote_id
                );
            }
        } else {
            info!(
                "Accepting connection from {} before router ready",
                remote_id
            );
        }

        let remote_id = conn.remote_id();

        match Conn::accept_connection(conn.clone(), direct_connect_tx.clone()).await {
            Ok(remote_conn) => {
                debug!("Successfully accepted connection from {}", remote_id);
                let mut guard = peer.accept_conn.lock().await;
                if *guard != PeerState::Connecting {
                    debug!(
                        "Connection to {} is no longer in Connecting state, dropping accepted connection",
                        remote_id
                    );
                    remote_conn.drop().await;
                    return Ok(());
                }
                if remote_conn.is_alive().await {
                    *guard = PeerState::Connected(remote_conn.clone());
                } else {
                    *guard = PeerState::NoConnection;
                    debug!(
                        "Accepted connection from {} is not alive after establishment, dropping",
                        remote_id
                    );
                    remote_conn.drop().await;
                    return Err(anyhow::anyhow!(
                        "Accepted connection from {} is not alive after establishment",
                        remote_id
                    ));
                }
                Ok(())
            }
            Err(err) => {
                error!("Failed to accept connection from {}: {}", remote_id, err);

                let mut guard = peer.accept_conn.lock().await;
                *guard = PeerState::NoConnection;
                conn.close(VarInt::from_u32(411), b"Failed to accept connection");
                Err(anyhow::anyhow!(
                    "Failed to accept connection from {}: {}",
                    remote_id,
                    err
                ))
            }
        }
    }

    async fn route_packet(&mut self, to: EndpointId, pkg: DirectMessage) -> Result<()> {
        trace!("Routing packet to {}", to);
        match self.peers.entry(to) {
            Entry::Occupied(entry) => {
                let peer = entry.get();

                // decide which conn to use based on connection availability

                // we send over the one with the closest last_keep_alive to ensure we don't send over a connection that might be dead but hasn't been cleaned up yet.
                let last_keep_alive_open =
                    if let PeerState::Connected(conn) = &*peer.open_conn.lock().await {
                        Some(conn.last_keep_alive().await)
                    } else {
                        None
                    };
                let last_keep_alive_accept =
                    if let PeerState::Connected(conn) = &*peer.accept_conn.lock().await {
                        Some(conn.last_keep_alive().await)
                    } else {
                        None
                    };

                let conns_to_attempt = match (last_keep_alive_open, last_keep_alive_accept) {
                    (Some(open_time), Some(accept_time)) => {
                        if open_time > accept_time {
                            vec![&peer.open_conn, &peer.accept_conn]
                        } else {
                            vec![&peer.accept_conn, &peer.open_conn]
                        }
                    }
                    (Some(_), None) => {
                        vec![&peer.open_conn]
                    }
                    (None, Some(_)) => {
                        debug!(
                            "Routing packet to {} over accept connection (open connection not available)",
                            to
                        );
                        vec![&peer.accept_conn]
                    }
                    (None, None) => {
                        debug!("No available connections to {}, cannot route packet", to);
                        return Err(anyhow::anyhow!(
                            "Failed to write packet to peer {}: no open connections",
                            to
                        ));
                    }
                };

                for conn in conns_to_attempt {
                    let guard = conn.lock().await;
                    if let PeerState::Connected(conn) = &*guard
                        && conn.is_alive().await
                        && conn.write(pkg.clone()).await.is_ok()
                    {
                        return Ok(());
                    }
                }
                error!("Failed to write packet to peer {}: no open connections", to);
                Err(anyhow::anyhow!(
                    "Failed to write packet to peer {}: no open connections",
                    to
                ))
            }
            Entry::Vacant(entry) => {
                info!("No active connection to {}, initiating new connection", to);
                entry.insert(Default::default());
                Err(anyhow::anyhow!(
                    "No active connection to {}, initiating new connection",
                    to
                ))
            }
        }
    }

    async fn ensure_peer(&mut self, to: EndpointId) -> Result<()> {
        if self.peers.contains_key(&to) {
            return Ok(());
        }

        info!(
            "No active connection to {}, initiating new connection (ensure_peer)",
            to
        );
        self.peers.insert(to, Default::default());
        Ok(())
    }

    pub async fn get_peer_state(&self, endpoint_id: EndpointId) -> Result<InnerConnState> {
        let peer = self
            .peers
            .get(&endpoint_id)
            .ok_or(anyhow::anyhow!("no connection to peer"))?;

        let conns = vec![&peer.accept_conn, &peer.open_conn];

        for conn in conns.clone() {
            let guard = conn.lock().await;
            if let PeerState::Connected(conn) = &*guard
                && conn.is_alive().await
            {
                return Ok(InnerConnState::Open);
            }
        }
        for conn in conns {
            let guard = conn.lock().await;
            if let PeerState::Connecting = &*guard {
                return Ok(InnerConnState::Connecting);
            }
        }
        Ok(InnerConnState::Closed)
    }

    pub async fn get_endpoint(&self) -> Result<iroh::endpoint::Endpoint> {
        Ok(self.endpoint.clone())
    }

    fn set_router(&mut self, router: Router) {
        self.router = Some(router);
    }

    pub async fn close(&mut self) -> Result<()> {
        for (_, conn) in self.peers.drain() {
            for conn in [&conn.accept_conn, &conn.open_conn] {
                let guard = conn.lock().await;
                if let PeerState::Connected(conn) = &*guard {
                    conn.drop().await;
                }
            }
        }
        Ok(())
    }
}

impl ProtocolHandler for Direct {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        info!("ProtocolHandler: new conn: {}", connection.remote_id());
        let _ = self.handle_connection(connection).await;
        Ok(())
    }
}
