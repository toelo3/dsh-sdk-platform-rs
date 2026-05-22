//! Kafka utilities for DSH.
//!
//! This module provides the [protobuf](https://protobuf.dev/) message types used to encode and
//! decode DSH Kafka message envelopes, along with helpers for working with DSH topics.
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
//! use dsh_sdk::prost::Message as _;
//! use dsh_sdk::utils::kafka::{DataEnvelope, data_envelope::Kind::Payload};
//!
//! let envelope = DataEnvelope {
//!     tracing: Default::default(),
//!     kind: Some(Payload(b"hello".to_vec())),
//! };
//! let bytes = envelope.encode_to_vec();
//! ```
//!
//! # Topic utilities
//!
//! [`reduce_topic_prefix`] truncates an MQTT-style topic string to a given depth, which is
//! required when computing the partition for the DSH
//! [topic-level partitioner](https://docs.kpn-dsh.com/reference/kafka-on-dsh/kafka-partitioners/#topic-level-partitioner).

/// Re-exported so downstream crates can use [`prost::Message`] without adding `prost` as a
/// direct dependency.
pub use prost;

include!(concat!(env!("OUT_DIR"), "/com.kpn.dsh.messages.common.rs"));

/// Reduces an MQTT topic's depth to `depth` levels.
///
/// The DSH's [topic-level
/// partioner](https://docs.kpn-dsh.com/reference/kafka-on-dsh/kafka-partitioners/#topic-level-partitioner)
/// uses the first `depth` levels of the MQTT topic to calculate the partition, where `depth` is
/// defined by the “topic level depth” setting of the DSH stream. It assumes the MQTT topic in
/// question does not have a leading forward slash (/).
///
/// a `depth` of `0` returns the entire topic.
pub fn reduce_topic_prefix(topic: &[u8], depth: usize) -> &[u8] {
    if depth == 0 {
        return topic;
    }

    let mut cur_depth = 1;
    for (i, &b) in topic.iter().enumerate() {
        if b == b'/' {
            if cur_depth == depth {
                return &topic[..i];
            }
            cur_depth += 1;
        }
    }
    topic
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use rdkafka::message::ToBytes;

    use super::*;

    #[test]
    fn prefix_depths() {
        assert_eq!(
            std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), 0)).unwrap(),
            "a/b/c"
        );
        assert_eq!(
            std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), 1)).unwrap(),
            "a"
        );
        assert_eq!(
            std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), 2)).unwrap(),
            "a/b"
        );
        assert_eq!(
            std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), 3)).unwrap(),
            "a/b/c"
        );
        assert_eq!(
            std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), 99)).unwrap(),
            "a/b/c"
        );
        assert_eq!(
            std::str::from_utf8(reduce_topic_prefix("abc".as_bytes(), 1)).unwrap(),
            "abc"
        );
    }

    #[test]
    fn dsh_key_envelope() {
        let key_envelope = KeyEnvelope {
            header: Some(KeyHeader {
                identifier: Some(Identity {
                    tenant: "test".to_string(),
                    publisher: todo!(),
                }),
                retained: true,
                qos: 1,
            }),
            key: "foo".to_string(),
        };
        key_envelope.encode_to_vec().to_bytes();
    }
}
