import time
import json
import subprocess
from kafka import KafkaProducer

ROUTER_IP = '192.168.207.134'
COMMUNITY = 'public'
IF_INDEX = '3'  # <<< เปลี่ยนเป็น ifIndex จริงของคุณ
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'snmp'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_snmp_value(oid):
    try:
        # เรียกใช้คำสั่ง snmpget ของระบบโดยตรง
        cmd = ["snmpget", "-v", "2c", "-c", COMMUNITY, ROUTER_IP, oid]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3)
        if result.returncode == 0:
            # ดึงเฉพาะตัวเลขออกจากผลลัพธ์ (เช่น Counter32: 12345678 -> 12345678)
            output = result.stdout.strip()
            val = output.split()[-1]
            return int(val)
    except Exception as e:
        print(f"[Error] SNMP Fetch failed: {e}")
    return None

print("Starting SNMP Collector...")
while True:
    in_bytes = get_snmp_value(f'1.3.6.1.2.1.2.2.1.10.{IF_INDEX}')
    out_bytes = get_snmp_value(f'1.3.6.1.2.1.2.2.1.16.{IF_INDEX}')
    
    if in_bytes is not None and out_bytes is not None:
        payload = {
            'timestamp': time.time(),
            'if_index': IF_INDEX,
            'in_bytes': in_bytes,
            'out_bytes': out_bytes
        }
        producer.send(TOPIC_NAME, value=payload)
        print(f"[Producer] Sent: {payload}")
    else:
        print("[Producer] Failed to fetch SNMP data")
    
    time.sleep(2)
