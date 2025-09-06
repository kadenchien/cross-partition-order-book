use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use crate::types::order::Order;
use crate::types::order_book::{OrderBook, PriceLevel};
use crate::types::match_event::MatchEvent;

pub struct MatchingEngine {
    pub order_books: std::collections::HashMap<String, OrderBook>,
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self {
            order_books: std::collections::HashMap::new(),
        }
    }

    pub fn process_order(&mut self, mut order: Order) -> Vec<MatchEvent> {
        let mut matches = Vec::new();
        
        // Get or create order book for this instrument
        let order_book = self.order_books
            .entry(order.instrument.clone())
            .or_insert_with(|| OrderBook::new(order.instrument.clone()));

        // Try to match the order
        if order.is_buy() {
            matches.extend(self.match_buy_order(order_book, &mut order));
        } else {
            matches.extend(self.match_sell_order(order_book, &mut order));
        }

        // Add remaining quantity to order book if not fully filled
        if !order.is_filled() {
            order_book.add_order(order);
        }

        // Clean up empty price levels
        order_book.cleanup_empty_levels();

        matches
    }

    fn match_buy_order(&self, order_book: &mut OrderBook, buy_order: &mut Order) -> Vec<MatchEvent> {
        let mut matches = Vec::new();
        
        // Get ask prices that can be matched (price <= buy_order.price)
        let matchable_ask_prices: Vec<i64> = order_book.asks.keys()
            .filter(|&&ask_price| (ask_price as f64 / 100.0) <= buy_order.price)
            .cloned()
            .collect();

        for ask_price_key in matchable_ask_prices {
            if buy_order.is_filled() {
                break;
            }

            let ask_price = ask_price_key as f64 / 100.0;
            
            if let Some(ask_level) = order_book.asks.get_mut(&ask_price_key) {
                matches.extend(self.pro_rata_match(buy_order, ask_level, ask_price));
                ask_level.remove_filled_orders();
            }
        }

        matches
    }

    fn match_sell_order(&self, order_book: &mut OrderBook, sell_order: &mut Order) -> Vec<MatchEvent> {
        let mut matches = Vec::new();
        
        // Get bid prices that can be matched (price >= sell_order.price)
        let matchable_bid_prices: Vec<i64> = order_book.bids.keys()
            .filter(|&&bid_price| (bid_price as f64 / 100.0) >= sell_order.price)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev() // Process highest bids first
            .collect();

        for bid_price_key in matchable_bid_prices {
            if sell_order.is_filled() {
                break;
            }

            let bid_price = bid_price_key as f64 / 100.0;
            
            if let Some(bid_level) = order_book.bids.get_mut(&bid_price_key) {
                matches.extend(self.pro_rata_match(sell_order, bid_level, bid_price));
                bid_level.remove_filled_orders();
            }
        }

        matches
    }

    fn pro_rata_match(&self, aggressive_order: &mut Order, price_level: &mut PriceLevel, match_price: f64) -> Vec<MatchEvent> {
        let mut matches = Vec::new();
        let mut remaining_aggressive_qty = aggressive_order.quantity;
        
        if remaining_aggressive_qty == 0 || price_level.total_quantity == 0 {
            return matches;
        }

        // Calculate pro-rata allocation for each order at this price level
        let mut allocations = Vec::new();
        let mut total_allocated = 0u32;

        // First pass: calculate proportional allocations
        for order in &price_level.orders {
            if order.is_filled() {
                continue;
            }

            let proportion = order.quantity as f64 / price_level.total_quantity as f64;
            let allocated = ((remaining_aggressive_qty as f64 * proportion).floor() as u32)
                .min(order.quantity);
            
            allocations.push(allocated);
            total_allocated += allocated;
        }

        // Second pass: distribute any remaining quantity due to rounding
        let mut remaining_to_distribute = remaining_aggressive_qty.saturating_sub(total_allocated);
        let mut order_index = 0;
        
        while remaining_to_distribute > 0 && order_index < price_level.orders.len() {
            let order = &price_level.orders[order_index];
            if !order.is_filled() && allocations[order_index] < order.quantity {
                let additional = std::cmp::min(remaining_to_distribute, order.quantity - allocations[order_index]);
                allocations[order_index] += additional;
                remaining_to_distribute -= additional;
            }
            order_index += 1;
        }

        // Third pass: execute the trades
        for (i, order) in price_level.orders.iter_mut().enumerate() {
            if order.is_filled() || allocations[i] == 0 {
                continue;
            }

            let trade_quantity = allocations[i];
            let aggressive_fill = aggressive_order.fill(trade_quantity);
            let passive_fill = order.fill(trade_quantity);
            
            // Both should be equal, but use the minimum to be safe
            let actual_trade_quantity = std::cmp::min(aggressive_fill, passive_fill);

            if actual_trade_quantity > 0 {
                let match_event = MatchEvent {
                    id: Uuid::new_v4().to_string(),
                    instrument: aggressive_order.instrument.clone(),
                    buyer_order_id: if aggressive_order.is_buy() {
                        aggressive_order.id.clone()
                    } else {
                        order.id.clone()
                    },
                    seller_order_id: if aggressive_order.is_sell() {
                        aggressive_order.id.clone()
                    } else {
                        order.id.clone()
                    },
                    price: match_price,
                    quantity: actual_trade_quantity,
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                };

                matches.push(match_event);
            }

            if aggressive_order.is_filled() {
                break;
            }
        }

        matches
    }
}

impl Default for MatchingEngine {
    fn default() -> Self {
        Self::new()
    }
}
