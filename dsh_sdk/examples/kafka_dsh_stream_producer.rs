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

use std::time::Duration;

use dsh_sdk::{DshKafkaConfig, protocol_adapters::kafka_protocol::DshPartitioner};
use log::info;
use rdkafka::{
    ClientConfig, ClientContext, config::FromClientConfigAndContext, producer::{FutureProducer, FutureRecord, ProducerContext}, util::DefaultRuntime
};

const TOTAL_MESSAGES: usize = 10;

struct DshContext {
    pub partitioner: DshPartitioner,
}

// `ProducerContext` requires `ClientContext`, default implementations will do.
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

async fn produce(producer: FutureProducer<DshContext>, topic: &str) {
    for key in 0..TOTAL_MESSAGES {
        let payload = format!("hello world {}", key);
        let record = producer
            .send(
                FutureRecord::to(topic)
                    .payload(payload.as_bytes())
                    .key(&key.to_be_bytes()),
                Duration::from_secs(10),
            )
            .await;
        match record {
            Ok(_) => info!("Message {} sent to {}", key, topic),
            Err(e) => info!("Error sending message: {}", e.0),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start logger to Stdout to show what is happening
    env_logger::builder()
        .filter(Some("dsh_sdk"), log::LevelFilter::Debug)
        .target(env_logger::Target::Stdout)
        .init();

    let sdk = dsh_sdk::Dsh::get();

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

    // Produce messages towards topic
    produce(producer, stream.name()).await;

    Ok(())
}
