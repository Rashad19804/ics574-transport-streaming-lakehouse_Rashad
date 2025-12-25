FROM apache/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir pandas psycopg2-binary

# Copy jobs
WORKDIR /app
COPY jobs/ /app/jobs/

# Spark image already has /opt/spark
USER 185
