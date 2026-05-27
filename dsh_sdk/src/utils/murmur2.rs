//! Murmur2 hashing for Kafka partition assignment.
//!
//! Implements the Murmur2 hash algorithm used by Kafka's default partitioner to map message keys
//! to partition numbers. This module is used internally by the DSH partitioner; it is not part of
//! the public API.
//!
//! The algorithm is specialised for little-endian aligned 32-bit inputs and mirrors the original
//! C++ code by Austin Appleby (public domain,
//! [SMHasher](https://github.com/aappleby/smhasher)), using the same seed and constants as the
//! [librdkafka](https://github.com/confluentinc/librdkafka/blob/54e000ef4ccabda759a1cf4fcbc06ba9edb193bb/src/rdmurmur2.c#L58)
//! and [Kafka Java client](https://github.com/apache/kafka/blob/62db165d2af99c489010688fcaa4addf4c398964/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L505)
//! implementations.
const SEED: u32 = 0x9747_b28c;
const M: u32 = 0x5bd1_e995;
const R: u32 = 24;

/// Murmur2 Hashing algorithm
///
/// Specialized for little-endian aligned 32-bit.
///
/// Implementation mirrors the original C++ code by Austin Appleby, placed in the public domain.
/// Found on [SMHasher](https://github.com/aappleby/smhasher).
///
/// Uses a Kafka-specific seed as found in common implementations like
/// [librdkafka](https://github.com/confluentinc/librdkafka/blob/54e000ef4ccabda759a1cf4fcbc06ba9edb193bb/src/rdmurmur2.c#L58)
/// and [Java
/// SDK](https://github.com/apache/kafka/blob/62db165d2af99c489010688fcaa4addf4c398964/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L505)
///
/// Values for `M` and `R` constants taken from implementation.
#[inline]
pub(crate) fn murmur2_32(data: &[u8]) -> u32 {
    let len = data.len() as u32;
    let mut h: u32 = SEED ^ len;

    let mut i = 0usize;
    let nblocks = len / 4;

    for _ in 0..nblocks {
        // Mix all 32-bit aligned blocks
        let k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
        i += 4;

        let mut k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h = h.wrapping_mul(M);
        h ^= k;
    }

    // Tail, at most 3 bytes, or nblocks would have increased, leading to another iteration
    // above
    let tail = &data[i..];
    match tail.len() {
        3 => {
            h ^= (tail[2] as u32) << 16;
            h ^= (tail[1] as u32) << 8;
            h ^= tail[0] as u32;
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= (tail[1] as u32) << 8;
            h ^= tail[0] as u32;
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= tail[0] as u32;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    // Final avalanche
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    h
}

/// Accounts for the i32 representation of the hash in Java's Kafka implementation by clearing
/// the sign bit.
#[inline]
pub(crate) fn to_positive(hash: u32) -> i32 {
    (hash & 0x7fff_ffff) as i32
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::from_str;

    use crate::protocol_adapters::kafka_protocol::utils::reduce_topic_prefix;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct GoldenCase {
        input: GoldenInput,
        expected: i32,
    }

    #[derive(Debug, Deserialize)]
    struct GoldenInput {
        topic: String,
        depth: usize,
        partitions: i32,
    }

    #[test]
    fn golden_samples() {
        let json = include_str!("../../tests/data/golden_data.json");
        let cases: Vec<GoldenCase> = from_str(json).expect("golden dataset invalid.");

        for (idx, case) in cases.iter().enumerate() {
            assert!(
                case.input.partitions > 0,
                "case {idx}: partitions must be > 0"
            );
            let t = reduce_topic_prefix(case.input.topic.as_bytes(), case.input.depth);
            let got = to_positive(murmur2_32(t)) % case.input.partitions;
            assert_eq!(
                got, case.expected,
                "topic={} depth={} partitions={}",
                case.input.topic, case.input.depth, case.input.partitions
            );
        }
    }
}
