//! This example demonstrates how to use the dsh_sdk crate to produce messages to a public [DSH
//! Stream](https://docs.kpn-dsh.com/reference/resources/dsh-stream/#dsh-stream) using the rdkafka
//! library.
//!
//! The messages produced with this examples are readable using the [Messaging
//! API](https://docs.kpn-dsh.com/platform-services/messaging-api/).
//!
//! The included `Dockerfile` builds an image that can be run on your DSH tenant, just make sure to
//! provide the `STREAM_NAME` through an environment variable, e.g. `stream.training`. This should
//! be a (public) stream your tenant has write access to.

use std::{collections::HashMap, time::Duration};

use dsh_sdk::{
    prost::Message,
    protocol_adapters::kafka_protocol::DshPartitioner,
    protocol_adapters::kafka_protocol::dsh_envelope::{
        data_envelope::Kind, identity::Publisher, DataEnvelope, Identity, KeyEnvelope, KeyHeader,
    },
    DshKafkaConfig,
};
use log::{debug, info};
use rdkafka::{
    config::FromClientConfigAndContext,
    message::ToBytes,
    producer::{FutureProducer, FutureRecord, ProducerContext},
    ClientConfig, ClientContext,
};
use tokio::time::sleep;

struct DshContext {
    pub partitioner: DshPartitioner,
}

impl ClientContext for DshContext {}

// implement `get_custom_partitioner`
impl ProducerContext<DshPartitioner> for DshContext {
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

    fn get_custom_partitioner(&self) -> std::option::Option<&DshPartitioner> {
        Some(&self.partitioner)
    }
}

async fn produce(
    producer: FutureProducer<DshContext>,
    topic: &str,
    identifier: Option<Identity>,
    retained: bool,
    qos: i32,
) {
    info!("start producing");

    let mut counter: usize = 0;
    loop {
        debug!("starting loop {counter}");
        // Create the key envelope
        let key_envelope = KeyEnvelope {
            header: Some(KeyHeader {
                identifier: identifier.clone(),
                retained,
                qos,
            }),
            key: counter.to_string(),
        };

        // Our payload
        let payload = format!("hello world {}", counter);

        // Create the data envelope
        let data_envelope = DataEnvelope {
            tracing: HashMap::new(),
            kind: Some(Kind::Payload(payload.as_bytes().to_owned())),
        };

        // Produce the record
        let record = producer
            .send(
                FutureRecord::to(topic)
                    .payload(data_envelope.encode_to_vec().to_bytes())
                    .key(key_envelope.encode_to_vec().to_bytes()),
                Duration::from_secs(10),
            )
            .await;

        match record {
            Ok(_) => info!("Message {} sent to {}", counter, topic),
            Err(e) => info!("Error sending message: {}", e.0),
        }

        counter += 1;

        sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start logger to Stdout to show what is happening
    env_logger::builder()
        .filter(Some("dsh_sdk"), log::LevelFilter::Debug)
        .filter(Some("dsh_stream_producer"), log::LevelFilter::Debug)
        .target(env_logger::Target::Stdout)
        .init();

    info!("Starting dsh-stream-producer");

    let sdk = dsh_sdk::Dsh::get();

    let identity = Identity {
        tenant: "accelerator".to_string(),
        publisher: Some(Publisher::Application("dsh-stream-producer".to_string())),
    };

    let stream = sdk
        .datastream()
        .get_stream(&std::env::var("STREAM_NAME").expect("`STREAM_NAME` should be set"))
        .expect("provided stream not found:");

    let partitioner = stream.partitioner_builder()?;

    let ctx = DshContext { partitioner };

    // Create a new producer from the RDkafka Client Config together with dsh_prodcer_config form DshKafkaConfig trait
    let producer: FutureProducer<DshContext> =
        FutureProducer::from_config_and_context(ClientConfig::new().set_dsh_producer_config(), ctx)
            .unwrap();

    info!("producer created");

    // Produce messages towards topic
    produce(producer, stream.write(), Some(identity), true, 1).await;

    Ok(())
}
