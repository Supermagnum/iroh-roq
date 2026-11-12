use anyhow::{ensure, Result};
use bytes::Bytes;
use iroh::endpoint::VarInt;
use rtp::packet::Packet as RtpPacket;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use webrtc_util::Unmarshal;

/// The receiving side of an RTP flow.
#[derive(Debug)]
pub struct ReceiveFlow {
    id: VarInt,
    cancel_token: CancellationToken,
    datagram_receiver: mpsc::Receiver<Bytes>,
}

impl ReceiveFlow {
    pub(crate) fn new(
        id: VarInt,
        receiver: mpsc::Receiver<Bytes>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            id,
            datagram_receiver: receiver,
            cancel_token,
        }
    }

    /// Returns the flow ID for this `ReceiveFlow`.
    pub fn flow_id(&self) -> VarInt {
        self.id
    }

    /// Reads the next available datagram payload (after flow-id), raw bytes.
    ///
    /// Use this for SRTP (or other AEAD) envelopes; [`Self::read_rtp`] unmarshals
    /// plain RTP.
    pub async fn read_bytes(&mut self) -> Result<Bytes> {
        ensure!(!self.cancel_token.is_cancelled(), "closed");
        let Some(bytes) = self.datagram_receiver.recv().await else {
            return Err(anyhow::anyhow!("session is closed"));
        };
        debug!(flow_id = %self.id, len = bytes.len(), "received datagram payload");
        Ok(bytes)
    }

    /// Reads the next available RTP packet.
    pub async fn read_rtp(&mut self) -> Result<RtpPacket> {
        let mut bytes = self.read_bytes().await?;
        let packet = RtpPacket::unmarshal(&mut bytes)?;
        debug!(flow_id = %self.id, "received packet");
        Ok(packet)
    }

    /// Close this flow
    pub fn close(&self) {
        self.cancel_token.cancel();
    }

    /// Is this flow closed?
    pub fn is_closed(&self) -> bool {
        self.cancel_token.is_cancelled()
    }
}
