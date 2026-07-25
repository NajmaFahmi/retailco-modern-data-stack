# Base image with Python 3.11
FROM python:3.11-slim

# Install Java (required by Spark) and curl, then clean up apt cache
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk curl && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Install Python dependencies first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download the GCS connector JAR required by Spark to read from and write to GCS
RUN mkdir -p /app/jars && \
    curl -L -o /app/jars/gcs-connector.jar \
    "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"

# Copy the entire project into the container
COPY . .