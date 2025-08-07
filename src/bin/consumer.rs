use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use rdkafka::ClientConfig;
use serde_json;
use cross_partition_order_book::types::order::Order;

#[tokio::main]
async fn main() {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "order-consumer-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["orders"])
        .expect("Can't subscribe to specified topic");

    println!("Waiting for orders...");

    let mut message_stream = consumer.stream();
    while let Some(message) = message_stream.next().await {
        match message {
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    Some(Ok(s)) => s,
                    Some(Err(_)) => "<invalid utf8>",
                    None => "<empty payload>",
                };

                if payload != "<empty payload>" && payload != "<invalid utf8>" {
                    match serde_json::from_str::<Order>(payload) {
                        Ok(order) => {
                            println!(
                                "Received order: {:?} (partition={}, offset={})",
                                order,
                                m.partition(),
                                m.offset()
                            );
                        }
                        Err(e) => eprintln!("Failed to parse order JSON: {}", e),
                    }
                } else {
                    println!("Received non-order payload: {}", payload);
                }
            }
            Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}
