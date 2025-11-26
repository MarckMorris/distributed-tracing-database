#!/bin/bash
echo "Starting Distributed Tracing System..."
docker-compose up -d
sleep 10
python src/distributed_tracer.py
