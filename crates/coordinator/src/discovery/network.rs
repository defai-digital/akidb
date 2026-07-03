//! libp2p network setup for discovery.

#[cfg(feature = "discovery")]
use std::time::Duration;

#[cfg(feature = "discovery")]
use anyhow::Result;
#[cfg(feature = "discovery")]
use libp2p::{
    core::upgrade, gossipsub, identify, mdns, noise, swarm::NetworkBehaviour, tcp, yamux, PeerId,
    Swarm, SwarmBuilder,
};
#[cfg(feature = "discovery")]
use tracing::info;

#[cfg(feature = "discovery")]
use super::config::DiscoveryConfig;

/// Combined network behaviour for AkiDB discovery
#[cfg(feature = "discovery")]
#[derive(NetworkBehaviour)]
pub struct AkiDbBehaviour {
    /// mDNS for local peer discovery
    pub mdns: mdns::tokio::Behaviour,
    /// Gossipsub for state dissemination
    pub gossipsub: gossipsub::Behaviour,
    /// Identify protocol for peer info exchange
    pub identify: identify::Behaviour,
}

/// Create a new libp2p swarm with the AkiDB behaviour
#[cfg(feature = "discovery")]
pub async fn create_swarm(config: &DiscoveryConfig) -> Result<Swarm<AkiDbBehaviour>> {
    // Generate or load identity keypair
    let local_key = if let Some(key_path) = &config.identity_path {
        load_or_create_identity(key_path)?
    } else {
        libp2p_identity::Keypair::generate_ed25519()
    };
    let local_peer_id = PeerId::from(local_key.public());

    info!("Local PeerId: {}", local_peer_id);

    // Build the swarm
    let swarm = SwarmBuilder::with_existing_identity(local_key.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            // Configure mDNS
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

            // Configure gossipsub
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(config.heartbeat_interval())
                .validation_mode(gossipsub::ValidationMode::Strict)
                .mesh_n_low(2)
                .mesh_n(4)
                .mesh_n_high(8)
                .build()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            // Configure identify
            let identify = identify::Behaviour::new(identify::Config::new(
                format!("/akidb/{}/1.0.0", config.namespace),
                key.public(),
            ));

            Ok(AkiDbBehaviour {
                mdns,
                gossipsub,
                identify,
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    Ok(swarm)
}

/// Load identity from file or create new one
#[cfg(feature = "discovery")]
fn load_or_create_identity(path: &std::path::Path) -> Result<libp2p_identity::Keypair> {
    use std::fs;
    use std::io::{Read, Write};

    if path.exists() {
        // Load existing identity
        let mut file = fs::File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let keypair = libp2p_identity::Keypair::from_protobuf_encoding(&bytes)?;
        info!("Loaded identity from {:?}", path);
        Ok(keypair)
    } else {
        // Create new identity
        let keypair = libp2p_identity::Keypair::generate_ed25519();
        let bytes = keypair.to_protobuf_encoding()?;

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = fs::File::create(path)?;
        file.write_all(&bytes)?;
        info!("Created new identity at {:?}", path);
        Ok(keypair)
    }
}

/// Get the local peer ID from a swarm
#[cfg(feature = "discovery")]
pub fn local_peer_id(swarm: &Swarm<AkiDbBehaviour>) -> &PeerId {
    swarm.local_peer_id()
}
