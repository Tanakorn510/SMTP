import asyncio
import json
import threading
import websockets
from kafka import KafkaConsumer

connected_clients = set()
last_data = {"timestamp": 0, "in_bytes": None, "out_bytes": None}

# ใช้ asyncio Event Loop สำหรับ Broadcast ข้าม Thread
main_loop = None

async def ws_handler(websocket):
    connected_clients.add(websocket)
    print(f"[WebSocket] Client connected! Total clients: {len(connected_clients)}")
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)
        print(f"[WebSocket] Client disconnected. Total clients: {len(connected_clients)}")

def send_to_clients(payload):
    if connected_clients and main_loop:
        msg = json.dumps(payload)
        for client in list(connected_clients):
            asyncio.run_coroutine_threadsafe(client.send(msg), main_loop)

def kafka_consumer_thread():
    global last_data
    consumer = KafkaConsumer(
        'snmp',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print("[Worker] Kafka Consumer Thread started successfully.")

    for msg in consumer:
        data = msg.value
        current_time = data.get('timestamp', 0)
        in_bytes = data.get('in_bytes', 0)
        out_bytes = data.get('out_bytes', 0)

        if last_data["in_bytes"] is not None and last_data["out_bytes"] is not None:
            time_diff = current_time - last_data["timestamp"]
            if time_diff > 0:
                in_diff = in_bytes - last_data["in_bytes"]
                out_diff = out_bytes - last_data["out_bytes"]

                if in_diff < 0: in_diff = 0
                if out_diff < 0: out_diff = 0

                in_mbps = round((in_diff * 8) / (time_diff * 1_000_000), 4)
                out_mbps = round((out_diff * 8) / (time_diff * 1_000_000), 4)

                import datetime
                time_str = datetime.datetime.fromtimestamp(current_time).strftime('%H:%M:%S')

                payload = {
                    "timestamp": time_str,
                    "in_mbps": in_mbps,
                    "out_mbps": out_mbps
                }
                print(f"[Consumer] Calculated Mbps: {payload}")
                send_to_clients(payload)

        last_data = {"timestamp": current_time, "in_bytes": in_bytes, "out_bytes": out_bytes}

async def main():
    global main_loop
    main_loop = asyncio.get_running_loop()

    # สตาร์ท Kafka Consumer ใน Thread แยกต่างหาก
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()

    async with websockets.serve(ws_handler, "0.0.0.0", 8765):
        print("[Worker] WebSocket Server running on ws://0.0.0.0:8765")
        await asyncio.Future()  # รันค้างไว้ตลอดเวลา

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped.")
