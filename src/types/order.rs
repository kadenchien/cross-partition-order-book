use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub instrument: String,
    pub side: String, // "buy" or "sell"
    pub price: f64,
    pub quantity: u32,
    pub timestamp: i64,
}