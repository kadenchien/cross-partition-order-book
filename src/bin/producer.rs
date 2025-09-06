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

    println!("Producing test orders to 'orders' topic...");

    // Create a mix of buy and sell orders that can potentially match
    let test_orders = vec![
        // Some buy orders
        ("AAPL", "buy", 150.00, 100),
        ("AAPL", "buy", 149.50, 200),
        ("AAPL", "buy", 149.00, 150),
        
        // Some sell orders at similar prices
        ("AAPL", "sell", 150.50, 100),
        ("AAPL", "sell", 150.00, 50),  // Should match with first buy order
        ("AAPL", "sell", 149.50, 100), // Should match with buy orders
        
        // More buy orders
        ("AAPL", "buy", 151.00, 300),  // Aggressive buy, should match sells
        
        // Different instrument
        ("MSFT", "buy", 300.00, 100),
        ("MSFT", "sell", 299.50, 150),
        ("MSFT", "buy", 300.50, 75),   // Should match with sell
        
        // Large orders for pro-rata testing
        ("AAPL", "sell", 148.00, 1000), // Large sell order
        ("AAPL", "buy", 148.00, 100),   // Multiple smaller buys at same price
        ("AAPL", "buy", 148.00, 200),
        ("AAPL", "buy", 148.00, 300),
        ("AAPL", "buy", 148.00, 150),
    ];

    for (i, (instrument, side, price, quantity)) in test_orders.iter().enumerate() {
        let order = Order::new(
            Uuid::new_v4().to_string(),
            instrument.to_string(),
            side.to_string(),
            *price,
            *quantity,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        );

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

        println!(
            "Sent order {}: {} {} {}@{} qty:{} -> partition:{}, status={:?}",
            i + 1,
            order.instrument,
            order.side,
            &order.id[..8], // Show first 8 chars of UUID
            order.price,
            order.quantity,
            partition,
            delivery_status.map(|(p, o)| format!("{}:{}", p, o)).unwrap_or_else(|e| format!("Error: {}", e))
        );

        // Small delay between orders to make it easier to follow
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("Done producing {} orders!", test_orders.len());
}