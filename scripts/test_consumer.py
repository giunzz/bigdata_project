import json
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
from kafka import KafkaConsumer

def main():
    topic = "energy_raw"
    bootstrap_servers = ["localhost:9092"]
    
    print(f"[*] Connecting to Kafka at {bootstrap_servers}...")
    print(f"[*] Listening to topic '{topic}' (Press Ctrl+C to stop)...")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    
    count = 0
    try:
        for msg in consumer:
            count += 1
            val = msg.value
            b_id = val.get("building_id")
            ts = val.get("timestamp")
            reading = val.get("meter_reading")
            temp = val.get("air_temperature")
            print(f"[{count:05d}] Partition: {msg.partition:02d} | Offset: {msg.offset:06d} | Building: {b_id} | Time: {ts} | Reading: {reading} kW | Temp: {temp}°C")
    except KeyboardInterrupt:
        print(f"\n[!] Stopped. Received {count} messages.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
