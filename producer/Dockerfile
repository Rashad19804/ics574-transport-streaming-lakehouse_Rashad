FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt confluent-kafka
COPY hsl_mqtt_to_kafka.py /app/hsl_mqtt_to_kafka.py
CMD ["python","/app/hsl_mqtt_to_kafka.py"]
