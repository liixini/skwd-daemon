use anyhow::{Context, Result};
use futures_util::StreamExt;
use librespot::core::authentication::Credentials;
use librespot::discovery::{DeviceType, Discovery};
use sha2::{Digest, Sha256};

pub async fn await_credentials(device_name: &str, client_id: &str) -> Result<Credentials> {
    let device_id = device_id_for(device_name);
    let mut discovery = Discovery::builder(device_id.clone(), client_id.to_string())
        .name(device_name.to_string())
        .device_type(DeviceType::Speaker)
        .launch()
        .map_err(|e| anyhow::anyhow!("discovery launch failed: {e}"))?;
    tracing::info!(
        "music: discovery listening for pairing as '{}' (device_id={})",
        device_name,
        &device_id[..8]
    );
    let creds = discovery
        .next()
        .await
        .context("discovery stream ended without credentials")?;
    discovery.shutdown().await;
    tracing::info!("music: discovery received credentials");
    Ok(creds)
}

fn device_id_for(name: &str) -> String {
    let digest = Sha256::digest(name.as_bytes());
    let hex = digest
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    hex[..40].to_string()
}
