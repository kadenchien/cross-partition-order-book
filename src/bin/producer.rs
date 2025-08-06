use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Connect to Kafka
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Producer creation error");

    println!("Producing 5 messages to 'orders' topic...");

    for i in 0..5 {
        let key = format!("key-{}", i);
        let payload = format!("hello kafka {}", i);

        let delivery_status = producer
            .send(
                FutureRecord::to("orders")
                    .key(&key)
                    .payload(&payload),
                Duration::from_secs(0),
            )
            .await;

        println!("Sent: key='{}', payload='{}', status={:?}", key, payload, delivery_status);
    }

    println!("Done producing!");
}
