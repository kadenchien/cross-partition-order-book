use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{Message, ClientConfig};
use serde_json;
use cross_partition_order_book::types::match_event::MatchEvent;

#[tokio::main]
async fn main() {
    println!("Starting Match Event Monitor...");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "match-event-monitor-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["match-events"])
        .expect("Can't subscribe to match-events topic");

    println!("Monitoring match events...");

    let mut message_stream = consumer.stream();
    while let Some(message) = message_stream.next().await {
        match message {
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    Some(Ok(s)) => s,
                    Some(Err(_)) => {
                        eprintln!("Invalid UTF-8 in message payload");
                        continue;
                    }
                    None => {
                        eprintln!("Empty message payload");
                        continue;
                    }
                };

                match serde_json::from_str::<MatchEvent>(payload) {
                    Ok(match_event) => {
                        println!(
                            "TRADE EXECUTED: {} | {} shares @ ${:.2} | Buyer: {} | Seller: {} | Time: {} (partition={}, offset={})",
                            match_event.instrument,
                            match_event.quantity,
                            match_event.price,
                            &match_event.buyer_order_id[..8], // Show first 8 chars of UUID
                            &match_event.seller_order_id[..8],
                            match_event.timestamp,
                            m.partition(),
                            m.offset()
                        );
                    }
                    Err(e) => {
                        eprintln!("Failed to parse match event JSON: {}", e);
                        eprintln!("Raw payload: {}", payload);
                    }
                }
            }
            Err(e) => {
                eprintln!("Kafka error: {}", e);
            }
        }
    }
}
