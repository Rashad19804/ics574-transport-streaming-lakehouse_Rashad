import json
import os
import ssl
import time

from confluent_kafka import Producer
import paho.mqtt.client as mqtt

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "hsl_stream")

HSL_MQTT_HOST = os.getenv("HSL_MQTT_HOST", "mqtt.hsl.fi")
HSL_MQTT_PORT = int(os.getenv("HSL_MQTT_PORT", "8883"))
HSL_MQTT_TOPIC = os.getenv("HSL_MQTT_TOPIC", "/hfp/v2/journey/ongoing/vp/#")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")


def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Connected to MQTT broker with rc={rc}. Subscribing to {HSL_MQTT_TOPIC}")
    client.subscribe(HSL_MQTT_TOPIC)


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)
        # HSL HFP payload is usually {"VP": {...}}; keep only VP to simplify schema.
        vp = data.get("VP")
        if not vp:
            return
        producer.produce(KAFKA_TOPIC, json.dumps(vp).encode("utf-8"), callback=delivery_report)
        producer.poll(0)
    except Exception as e:
        print(f"Error processing message: {e}")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

# TLS (mqtt.hsl.fi uses 8883)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
client.tls_insecure_set(False)

while True:
    try:
        print(f"Connecting to MQTT {HSL_MQTT_HOST}:{HSL_MQTT_PORT} ...")
        client.connect(HSL_MQTT_HOST, HSL_MQTT_PORT, keepalive=60)
        client.loop_forever()
    except Exception as e:
        print(f"MQTT connection error: {e}. Retrying in 5s")
        time.sleep(5)
