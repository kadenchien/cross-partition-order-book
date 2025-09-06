use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, ClientConfig};
use std::collections::HashMap;
use std::time::Duration;
use serde_json;
use cross_partition_order_book::types::order::Order;
use cross_partition_order_book::types::match_event::MatchEvent;
use cross_partition_order_book::utils::matching_engine::MatchingEngine;

#[tokio::main]
async fn main() {
    println!("Starting Order Matching Engine...");

    // Create Kafka consumer for orders
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "matching-engine-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    // Create Kafka producer for match events
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed");

    // Subscribe to orders topic
    consumer
        .subscribe(&["orders"])
        .expect("Can't subscribe to orders topic");

    // Initialize matching engines per partition
    let mut matching_engines: HashMap<i32, MatchingEngine> = HashMap::new();

    println!("Matching engine ready. Waiting for orders...");

    let mut message_stream = consumer.stream();
    while let Some(message) = message_stream.next().await {
        match message {
            Ok(m) => {
                let partition = m.partition();
                
                // Get or create matching engine for this partition
                let matching_engine = matching_engines
                    .entry(partition)
                    .or_insert_with(MatchingEngine::new);

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

                // Parse the order
                match serde_json::from_str::<Order>(payload) {
                    Ok(order) => {
                        println!(
                            "Processing order: {} {} {}@{} qty:{} (partition={})",
                            order.instrument,
                            order.side,
                            order.id,
                            order.price,
                            order.quantity,
                            partition
                        );

                        // Process the order through the matching engine
                        let matches = matching_engine.process_order(order);

                        // Publish match events
                        for match_event in matches {
                            match serde_json::to_string(&match_event) {
                                Ok(match_payload) => {
                                    let delivery_result = producer
                                        .send(
                                            FutureRecord::to("match-events")
                                                .key(&match_event.instrument)
                                                .payload(&match_payload)
                                                .partition(partition), // Same partition as the order
                                            Duration::from_secs(1),
                                        )
                                        .await;

                                    match delivery_result {
                                        Ok((partition, offset)) => {
                                            println!(
                                                "Published match: {} traded {} {} @ {} (partition={}, offset={})",
                                                match_event.instrument,
                                                match_event.quantity,
                                                match_event.buyer_order_id,
                                                match_event.price,
                                                partition,
                                                offset
                                            );
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to publish match event: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to serialize match event: {}", e);
                                }
                            }
                        }

                        // Print order book status
                        if let Some(order_book) = matching_engine.order_books.get(&order.instrument) {
                            let best_bid = order_book.get_best_bid();
                            let best_ask = order_book.get_best_ask();
                            println!(
                                "Order book {}: Bid={:?} Ask={:?} (partition={})",
                                order_book.instrument, best_bid, best_ask, partition
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse order JSON: {}", e);
                    }
                }

                // Commit the message
                if let Err(e) = consumer.commit_message(&m, rdkafka::consumer::CommitMode::Async) {
                    eprintln!("Failed to commit message: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Kafka error: {}", e);
            }
        }
    }
}
