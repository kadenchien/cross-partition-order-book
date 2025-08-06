use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::Message;

#[tokio::main]
async fn main() {
    // Connect to Kafka
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "test-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["orders"])
        .expect("Can't subscribe to specified topic");

    println!("Waiting for messages on 'orders'...");

    let mut message_stream = consumer.stream();
    while let Some(message) = message_stream.next().await {
        match message {
        Ok(m) => {
            let payload = match m.payload_view::<str>() {
                Some(Ok(s)) => s,
                Some(Err(_)) => "<invalid utf8>",
                None => "<empty payload>",
            };
            println!(
                "Received message: key={:?}, payload='{}', partition={}, offset={}",
                m.key(),
                payload,
                m.partition(),
                m.offset()
            );
        }
        Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}
