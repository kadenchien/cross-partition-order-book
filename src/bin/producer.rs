use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use serde_json;
use cross_partition_order_book::types::order::Order;
use cross_partition_order_book::utils::partitioner::custom_partition;

#[tokio::main]
async fn main() {
    // Connect to Kafka
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Producer creation error");

    println!("Producing 5 messages to 'orders' topic...");

    for i in 0..5 {
        let order = Order {
            id: Uuid::new_v4().to_string(),
            instrument: format!("AAPL"),
            side: if i % 2 == 0 { "buy" } else { "sell" }.to_string(),
            price: 150.0 + i as f64,
            quantity: 100 + i * 10,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        };

        let payload = serde_json::to_string(&order).expect("Failed to serialize order");

        let partition = custom_partition(&order.instrument, 8);

        let delivery_status = producer
            .send(
                FutureRecord::to("orders")
                    .key(&order.instrument)
                    .payload(&payload)
                    .partition(partition),
                Duration::from_secs(0),
            )
            .await;

        println!("Sent order: {:?}, status={:?}", order, delivery_status);
    }

    println!("Done producing!");
}