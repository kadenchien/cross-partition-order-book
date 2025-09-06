use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Order {
    pub id: String,
    pub instrument: String,
    pub side: String, // "buy" or "sell"
    pub price: f64,
    pub quantity: u32,
    pub original_quantity: u32,
    pub timestamp: i64,
}

impl Order {
    pub fn new(id: String, instrument: String, side: String, price: f64, quantity: u32, timestamp: i64) -> Self {
        Self {
            id,
            instrument,
            side,
            price,
            original_quantity: quantity,
            quantity,
            timestamp,
        }
    }

    pub fn is_buy(&self) -> bool {
        self.side == "buy"
    }

    pub fn is_sell(&self) -> bool {
        self.side == "sell"
    }

    pub fn fill(&mut self, quantity: u32) -> u32 {
        let filled = std::cmp::min(self.quantity, quantity);
        self.quantity -= filled;
        filled
    }

    pub fn is_filled(&self) -> bool {
        self.quantity == 0
    }
}