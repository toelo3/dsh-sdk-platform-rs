pub fn reduce_topic_prefix<'a>(topic: &'a[u8], depth: &'a usize) -> &'a [u8] {
    if *depth == 0 {
        return topic;
    }

    let mut cur_depth = 1;
    for (i, &b) in topic.iter().enumerate() {
        if b == b'/' {
            if cur_depth == *depth {
                return &topic[..i];
            }
            cur_depth += 1;
        }
    }
    topic
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_examples() {
        assert_eq!(std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), &0)).unwrap(), "a/b/c");
        assert_eq!(std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), &1)).unwrap(), "a");
        assert_eq!(std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), &2)).unwrap(), "a/b");
        assert_eq!(std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), &3)).unwrap(), "a/b/c");
        assert_eq!(std::str::from_utf8(reduce_topic_prefix("a/b/c".as_bytes(), &99)).unwrap(), "a/b/c");
        assert_eq!(std::str::from_utf8(reduce_topic_prefix("abc".as_bytes(), &1)).unwrap(), "abc");
    }
}
