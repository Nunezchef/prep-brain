#!/bin/bash
set -e

echo "Starting Prep-Brain in Docker..."

if ! command -v docker &> /dev/null; then
    echo "Error: docker could not be found. Please install Docker Desktop."
    exit 1
fi

# Check if Ollama is running
if ! pgrep -x "ollama" > /dev/null; then
    echo "Ollama is not running. Starting Ollama..."
    # Start Ollama in the background and detach
    nohup ollama serve > logs/ollama.log 2>&1 &
    echo "Ollama started in background."
    sleep 2
else
    echo "Ollama is already running."
fi

mkdir -p run
if command -v docker-compose &> /dev/null; then
    docker-compose up --build
else
    docker compose up --build
fi
