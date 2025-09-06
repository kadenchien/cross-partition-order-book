use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchEvent {
    pub id: String,
    pub instrument: String,
    pub buyer_order_id: String,
    pub seller_order_id: String,
    pub price: f64,
    pub quantity: u32,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialFill {
    pub order_id: String,
    pub filled_quantity: u32,
    pub remaining_quantity: u32,
}
