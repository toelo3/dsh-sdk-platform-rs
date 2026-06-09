//! Kafka utilities for DSH.
//!
//! This module provides utilities to work with Kafka on DSH.
//!
//! # Topic utilities
//!
//! [`reduce_topic_prefix`] truncates an MQTT-style topic string to a given depth, which is
//! required when computing the partition for the DSH
//! [topic-level partitioner](https://docs.kpn-dsh.com/reference/kafka-on-dsh/kafka-partitioners/#topic-level-partitioner).

use crate::utils::murmur2::{murmur2_32, to_positive};

/// Reduces an MQTT topic's depth to `depth` levels.
///
/// The DSH's [topic-level
/// partioner](https://docs.kpn-dsh.com/reference/kafka-on-dsh/kafka-partitioners/#topic-level-partitioner)
/// uses the first `depth` levels of the MQTT topic to calculate the partition, where `depth` is
/// defined by the “topic level depth” setting of the DSH stream. It assumes the MQTT topic in
/// question does not have a leading forward slash (/).
///
/// a `depth` of `0` returns the entire topic.
pub(crate) fn reduce_topic_prefix(topic: &[u8], depth: usize) -> &[u8] {
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

/// Calculcates the correct partition for a key, given the number of partitions
///
/// Uses murmur2_32 for hashing, replicating the official Kafka Java client. Converts the hash
/// result to a positive integer and casts to i32 to ensure compatibility with Java's modulo
/// arithmetic behaviour.
pub(crate) fn partition(key: &[u8], partition_count: usize) -> i32 {
    to_positive(murmur2_32(key)) % partition_count as i32
}

#[cfg(test)]
mod tests {
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
}
