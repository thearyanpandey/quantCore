import redis
import json
import numpy as np
import time
import uuid

# --- Configuration ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
STREAM_KEYS = ['market_data:BTCUSDT', 'market_data:ETHUSDT']
CONSUMER_GROUP = 'ai_group'
CONSUMER_NAME = 'python_brain_1'

# Strategy Parameters
SHORT_WINDOW = 5   # Number of ticks for fast average
LONG_WINDOW = 20   # Number of ticks for slow average

# Internal state to hold price history per symbol
# Structure: { "BTCUSDT": [price1, price2, ...], "ETHUSDT": [...] }
price_history = {key.split(":")[1]: [] for key in STREAM_KEYS}

def connect_redis():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Create Consumer Group for each stream if it doesn't exist
    for stream in STREAM_KEYS:
        try:
            # '$' means "start reading from the latest message" (ignore old history)
            r.xgroup_create(stream, CONSUMER_GROUP, id='$', mkstream=True)
            print(f"✅ Created Consumer Group for {stream}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"ℹ️  Consumer Group for {stream} already exists.")
            else:
                raise e
    return r

def process_message(r, stream_name, message_id, message_data):
    # 1. Parse Data
    try:
        payload = json.loads(message_data['data'])
        symbol = payload['symbol']
        price = payload['price']
    except (KeyError, json.JSONDecodeError):
        print(f"❌ Corrupt data in {stream_name}")
        return

    # 2. Update History (State)
    history = price_history[symbol]
    history.append(price)
    
    # Keep history size managed (only keep what we need for the Long Window)
    if len(history) > LONG_WINDOW:
        history.pop(0)

    # 3. Run Strategy (Only if we have enough data)
    if len(history) == LONG_WINDOW:
        short_avg = np.mean(history[-SHORT_WINDOW:])
        long_avg = np.mean(history)

        # Detect Crossover (Simplified Logic for Demo)
        # In a real system, we'd check if previous tick was below and current is above.
        # Here we just check current state to generate frequent signals for testing.
        signal = None
        if short_avg > long_avg:
            signal = "BUY"
        elif short_avg < long_avg:
            signal = "SELL"

        if signal:
            print(f"🧠 ANALYSIS [{symbol}]: Price {price} | Short: {short_avg:.2f} | Long: {long_avg:.2f} -> {signal}")
            
            # 4. Publish Signal
            signal_payload = {
                "id": str(uuid.uuid4()),
                "symbol": symbol,
                "action": signal,
                "price": price,
                "timestamp": time.time()
            }
            
            # Write to the 'trade_signals' stream
            r.xadd('trade_signals', {'json': json.dumps(signal_payload)})

    # 5. Acknowledge message (tell Redis we are done with it)
    r.xack(stream_name, CONSUMER_GROUP, message_id)

def main():
    r = connect_redis()
    print("🧠 QuantCore Brain is Active. Waiting for data...")

    while True:
        try:
            # Read new messages from our assigned streams
            # block=1000 means wait 1 sec if no data, then loop again
            streams = {key: '>' for key in STREAM_KEYS}
            entries = r.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, streams, count=1, block=1000)

            if not entries:
                continue

            for stream_name, messages in entries:
                for message_id, message_data in messages:
                    process_message(r, stream_name, message_id, message_data)

        except KeyboardInterrupt:
            print("Stopping Brain...")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()