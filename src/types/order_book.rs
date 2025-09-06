use std::collections::{BTreeMap, VecDeque};
use crate::types::order::Order;

#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: f64,
    pub orders: VecDeque<Order>,
    pub total_quantity: u32,
}

impl PriceLevel {
    pub fn new(price: f64) -> Self {
        Self {
            price,
            orders: VecDeque::new(),
            total_quantity: 0,
        }
    }

    pub fn add_order(&mut self, order: Order) {
        self.total_quantity += order.quantity;
        self.orders.push_back(order);
    }

    pub fn remove_filled_orders(&mut self) {
        self.orders.retain(|order| !order.is_filled());
        self.total_quantity = self.orders.iter().map(|o| o.quantity).sum();
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
}

#[derive(Debug)]
pub struct OrderBook {
    pub instrument: String,
    // BTreeMap for sorted price levels - bids descending, asks ascending
    pub bids: BTreeMap<i64, PriceLevel>, // price as fixed-point integer (price * 100)
    pub asks: BTreeMap<i64, PriceLevel>,
}

impl OrderBook {
    pub fn new(instrument: String) -> Self {
        Self {
            instrument,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    fn price_to_key(price: f64) -> i64 {
        (price * 100.0).round() as i64
    }

    pub fn add_order(&mut self, order: Order) {
        let price_key = Self::price_to_key(order.price);
        
        if order.is_buy() {
            self.bids
                .entry(price_key)
                .or_insert_with(|| PriceLevel::new(order.price))
                .add_order(order);
        } else {
            self.asks
                .entry(price_key)
                .or_insert_with(|| PriceLevel::new(order.price))
                .add_order(order);
        }
    }

    pub fn get_best_bid(&self) -> Option<f64> {
        self.bids.keys().max().map(|&key| key as f64 / 100.0)
    }

    pub fn get_best_ask(&self) -> Option<f64> {
        self.asks.keys().min().map(|&key| key as f64 / 100.0)
    }

    pub fn cleanup_empty_levels(&mut self) {
        self.bids.retain(|_, level| !level.is_empty());
        self.asks.retain(|_, level| !level.is_empty());
    }
}
