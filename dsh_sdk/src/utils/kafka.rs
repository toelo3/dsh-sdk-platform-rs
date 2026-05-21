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
    }
}
