//! DSH Envelope
//!
//! This module provides the [protobuf](https://protobuf.dev/) message types used to encode and
//! decode DSH Kafka message envelopes required for public streams.
//!
//! # Envelope types
//!
//! DSH uses protobuf-encoded envelopes for both the key and value of every Kafka message. The
//! types in this module are generated from the DSH proto schema at `src/proto/dsh.proto`.
//! See the [DSH message envelope documentation](https://docs.kpn-dsh.com/reference/kafka-on-dsh/message-envelopes/)
//! for background on the format.
//!
//! | Type | Role |
//! |------|------|
//! | [`KeyEnvelope`] | Wraps the Kafka record key |
//! | [`KeyHeader`] | Metadata carried inside [`KeyEnvelope`]: identity, retained flag, QoS |
//! | [`Identity`] | Identifies the originating tenant and publisher |
//! | [`DataEnvelope`] | Wraps the Kafka record value |
//! | [`QoS`] | Quality-of-service level for a message |
//!
//! To serialise an envelope, bring [`prost::Message`] into scope and call
//! [`encode_to_vec`](prost::Message::encode_to_vec):
//!
//! ```
//! use dsh_sdk::protocol_adapters::kafka_protocol::dsh_envelope::{
//!     prost::Message as _,
//!     data_envelope::Kind::Payload, DataEnvelope
//! };
//!
//! let envelope = DataEnvelope {
//!     tracing: Default::default(),
//!     kind: Some(Payload(b"hello".to_vec())),
//! };
//! let bytes = envelope.encode_to_vec();
//! ```
//!

/// Re-exported so downstream crates can use [`prost::Message`] without adding `prost` as a
/// direct dependency.
pub use prost;

include!(concat!(env!("OUT_DIR"), "/com.kpn.dsh.messages.common.rs"));
