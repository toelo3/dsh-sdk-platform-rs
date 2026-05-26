#[cfg(feature = "rdkafka-config")]
use rdkafka::ClientConfig;
use rdkafka::ClientContext;
use rdkafka::producer::{Partitioner, ProducerContext};

use super::DshKafkaConfig;
use crate::Dsh;
use crate::utils::kafka::reduce_topic_prefix;
use crate::utils::murmur2::{murmur2_32, to_positive};

impl DshKafkaConfig for ClientConfig {
    fn set_dsh_consumer_config(&mut self) -> &mut Self {
        let dsh = Dsh::get();
        let client_id = dsh.client_id();
        let config = dsh.kafka_config();

        self.set("bootstrap.servers", config.kafka_brokers())
            .set("group.id", config.group_id())
            .set("client.id", client_id)
            .set(
                "enable.auto.commit",
                config.enable_auto_commit().to_string(),
            )
            .set("auto.offset.reset", config.auto_offset_reset());
        if let Some(session_timeout) = config.session_timeout() {
            self.set("session.timeout.ms", session_timeout.to_string());
        }
        if let Some(queued_buffering_max_messages_kbytes) =
            config.queued_buffering_max_messages_kbytes()
        {
            self.set(
                "queued.max.messages.kbytes",
                queued_buffering_max_messages_kbytes.to_string(),
            );
        }
        log::debug!("Consumer config: {:#?}", self);
        self.set_dsh_certificates();
        self
    }

    fn set_dsh_producer_config(&mut self) -> &mut Self {
        let dsh = Dsh::get();
        let client_id = dsh.client_id();
        let config = dsh.kafka_config();
        self.set("bootstrap.servers", config.kafka_brokers())
            .set("client.id", client_id);
        if let Some(batch_num_messages) = config.batch_num_messages() {
            self.set("batch.num.messages", batch_num_messages.to_string());
        }
        if let Some(queue_buffering_max_messages) = config.queue_buffering_max_messages() {
            self.set(
                "queue.buffering.max.messages",
                queue_buffering_max_messages.to_string(),
            );
        }
        if let Some(queue_buffering_max_kbytes) = config.queue_buffering_max_kbytes() {
            self.set(
                "queue.buffering.max.kbytes",
                queue_buffering_max_kbytes.to_string(),
            );
        }
        if let Some(queue_buffering_max_ms) = config.queue_buffering_max_ms() {
            self.set("queue.buffering.max.ms", queue_buffering_max_ms.to_string());
        }
        log::debug!("Producer config: {:#?}", self);
        self.set_dsh_certificates();
        self
    }

    fn set_dsh_group_id(&mut self, group_id: &str) -> &mut Self {
        let tenant = Dsh::get().tenant_name();
        if group_id.starts_with(tenant) {
            self.set("group.id", group_id)
        } else {
            self.set("group.id", format!("{}_{}", tenant, group_id))
        }
    }

    fn set_dsh_certificates(&mut self) -> &mut Self {
        let dsh = Dsh::get();
        if let Ok(certificates) = dsh.certificates() {
            self.set("security.protocol", "ssl")
                .set("ssl.key.pem", certificates.private_key_pem())
                .set(
                    "ssl.certificate.pem",
                    certificates.dsh_signed_certificate_pem(),
                )
                .set("ssl.ca.pem", certificates.dsh_ca_certificate_pem())
        } else {
            self.set("security.protocol", "plaintext")
        }
    }
}

// Wrapper enum for Partitioner implementations
pub enum RdkafkaPartitioner {
    Default(super::DefaultPartitioner),
    TopicLevel(super::TopicLevelPartitioner),
}

impl Partitioner for RdkafkaPartitioner {
    fn partition(
        &self,
        topic_name: &str,
        key: Option<&[u8]>,
        partition_cnt: i32,
        is_partition_available: impl Fn(i32) -> bool,
    ) -> i32 {
        match self {
            RdkafkaPartitioner::Default(p) => {
                p.partition(topic_name, key, partition_cnt, is_partition_available)
            }
            RdkafkaPartitioner::TopicLevel(p) => {
                p.partition(topic_name, key, partition_cnt, is_partition_available)
            }
        }
    }
}

/// Default partitioner for Kafka on DSH
///
/// Uses the full MQTT topic from the `key` as input to calculate the partition if provided,
/// otherwise defaults to 0.
impl Partitioner for super::DefaultPartitioner {
    fn partition(
        &self,
        _topic_name: &str,
        key: Option<&[u8]>,
        partition_cnt: i32,
        _is_partition_available: impl Fn(i32) -> bool,
    ) -> i32 {
        match key {
            Some(k) => to_positive(murmur2_32(k)) % partition_cnt,
            None => 0,
        }
    }
}

/// Topic Level partitioner for Kafka on DSH
///
/// We implement the Murmur2 hashing algorithm as is done in the librdkafka implementation.
///
/// Defaults to 0 when no key is provided.
impl Partitioner for super::TopicLevelPartitioner {
    fn partition(
        &self,
        _topic_name: &str,
        key: Option<&[u8]>,
        partition_cnt: i32,
        _is_partition_available: impl Fn(i32) -> bool,
    ) -> i32 {
        match key {
            Some(k) => {
                to_positive(murmur2_32(reduce_topic_prefix(k, self.partitioning_depth)))
                    % partition_cnt
            }
            None => 0,
        }
    }
}

pub struct MyContext {
    pub partitioner: RdkafkaPartitioner,
}

impl ClientContext for MyContext {}

impl ProducerContext<RdkafkaPartitioner> for MyContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::message::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        if let Err(e) = delivery_result {
            self.log(
                rdkafka::config::RDKafkaLogLevel::Warning,
                "",
                &format!("{e:?}"),
            );
        } else {
            self.log(
                rdkafka::config::RDKafkaLogLevel::Debug,
                "",
                "Record delivery success",
            );
        }
    }

    fn get_custom_partitioner(&self) -> std::option::Option<&RdkafkaPartitioner> {
        Some(&self.partitioner)
    }
}
