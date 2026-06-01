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
//!
//! The rdkafka::FutureProducer requires a `ProducerContext` with a `get_custom_partitioner`
//! implementation in order to provide our `DshPartitioner`. It is up to the user of the SDK to
//! create one, an example is provided here.

use std::{collections::HashMap, time::Duration};

use dsh_sdk::{
    prost::Message,
    protocol_adapters::kafka_protocol::{
        compute_partition,
        dsh_envelope::{
            data_envelope::Kind, identity::Publisher, DataEnvelope, Identity, KeyEnvelope,
            KeyHeader,
        },
        DshPartitioner,
    },
    DshKafkaConfig,
};
use log::{debug, info};
use rdkafka::{
    config::FromClientConfig,
    message::ToBytes,
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use tokio::time::sleep;

async fn produce(
    producer: &FutureProducer,
    topic: &str,
    identifier: Identity,
    retained: bool,
    qos: i32,
    partitioner: DshPartitioner,
    partition_count: usize,
) {
    info!("start producing");

    let mut counter: usize = 0;
    loop {
        debug!("starting loop {counter}");
        // MQTT topic
        let key = format!("foo/bar/count/{counter}");

        // Calculate partition
        let partition = compute_partition(key.as_bytes(), &partitioner, partition_count);

        // Create the key envelope
        let key_envelope = KeyEnvelope {
            header: Some(KeyHeader {
                identifier: Some(identifier.clone()),
                retained,
                qos,
            }),
            key,
        };

        // Our payload
        let payload = format!("hello world {}", counter);

        // Create the data envelope
        let data_envelope = DataEnvelope {
            tracing: HashMap::new(),
            kind: Some(Kind::Payload(payload.as_bytes().to_owned())),
        };

        // Produce the record, we must manually set the partition of the Kafka record
        let record = producer
            .send(
                FutureRecord::to(topic)
                    .payload(data_envelope.encode_to_vec().to_bytes())
                    .key(key_envelope.encode_to_vec().to_bytes())
                    .partition(partition),
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

    let partitioner = stream.partitioner()?;
    let partition_count = stream.partitions();

    // Create a new producer from the RDkafka Client Config together with dsh_prodcer_config form DshKafkaConfig trait
    let producer: FutureProducer =
        FutureProducer::from_config(ClientConfig::new().set_dsh_producer_config()).unwrap();

    info!("producer created");

    // Produce messages towards topic
    produce(
        &producer,
        stream.write(),
        identity,
        true,
        1,
        partitioner,
        partition_count,
    )
    .await;

    Ok(())
}
