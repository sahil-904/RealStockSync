from kafka import KafkaProducer
import json
import time
import yfinance as yf

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")  # JSON serialization
)

topic = "gail_stock_data"  # Kafka topic for GAIL stock
ticker = "GAIL.NS"  # Yahoo Finance symbol for GAIL (NSE)

# Fetch Stock Data
while True:
    stock = yf.download(ticker, period="1d", interval="1m")

    if stock.empty:
        print("⚠️ No data fetched, retrying...")
        time.sleep(5)
        continue  # Skip iteration if no data

    latest = stock.iloc[-1]  # Get latest row
    print(f"✅ Available Columns: {list(latest.index)}")  # Debugging step

    # Extract correct column values
    message = {
        "timestamp": str(stock.index[-1]),  # Convert Timestamp to string
        "ticker": "GAIL",
        "open": float(latest[("Open", ticker)]),  # Extract using correct multi-index keys
        "high": float(latest[("High", ticker)]),
        "low": float(latest[("Low", ticker)]),
        "close": float(latest[("Close", ticker)]),
        "volume": int(latest[("Volume", ticker)])
    }

    print(f"📤 Sending: {message}")
    producer.send(topic, value=message)
    producer.flush()

    time.sleep(5)  # Send data every 5 seconds
