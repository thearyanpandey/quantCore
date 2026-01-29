use futures_util::{StreamExt, SinkExt};
use redis::AsyncCommands; // Brings traits like xadd into scope
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

// 1. Define the Data Contract (The Shape of Data)
// We only care about specific fields from Binance to save bandwidth.
#[derive(Serialize, Deserialize, Debug)]
struct MarketTick {
    symbol: String,
    price: f64,
    quantity: f64,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Connect to Redis
    // We use the standard localhost port we opened in Docker.
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_multiplexed_async_connection().await?;
    println!("✅ Connected to Redis!");

    // 3. Connect to Binance WebSocket
    // We subscribe to TWO streams: BTC/USDT and ETH/USDT.
    let binance_url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade";
    let url = Url::parse(binance_url)?;

    println!("Attempting to connect to Binance...");
    let (ws_stream, _) = connect_async(url).await?;
    println!("✅ Connected to Binance WebSocket!");

    let (_, mut read) = ws_stream.split();

    // 4. The Main Loop (Ingest -> Parse -> Push)
    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                // Parse the raw JSON from Binance
                let v: Value = serde_json::from_str(&text)?;

                // Extract the data we need (Binance wraps data in a "data" object)
                if let Some(data) = v.get("data") {
                    let symbol = data["s"].as_str().unwrap_or("UNKNOWN").to_string();
                    let price = data["p"].as_str().unwrap().parse::<f64>().unwrap_or(0.0);
                    let quantity = data["q"].as_str().unwrap().parse::<f64>().unwrap_or(0.0);
                    let timestamp = data["T"].as_u64().unwrap_or(0);

                    // Create our Clean Data Object
                    let tick = MarketTick {
                        symbol: symbol.clone(),
                        price,
                        quantity,
                        timestamp,
                    };

                    // Serialize to JSON for Redis
                    let tick_json = serde_json::to_string(&tick)?;

                    // 5. Push to Redis Stream (XADD)
                    // Stream Key: market_data:{SYMBOL} (e.g., market_data:BTCUSDT)
                    // We use MAXLEN ~ 1000 to prevent Redis from filling up RAM indefinitely.
                    let stream_key = format!("market_data:{}", symbol);
                    let _: String = con.xadd_maxlen(
                        &stream_key,
                        redis::streams::StreamMaxlen::Approx(1000),
                        "*", // let Redis generate the ID
                        &[("data", &tick_json)],
                    ).await?;

                    println!("🚀 Pushed to {}: {:.2}", stream_key, price);
                }
            }
            Ok(_) => {} // Ignore Pings/Pongs
            Err(e) => println!("Error receiving message: {:?}", e),
        }
    }

    Ok(())
}