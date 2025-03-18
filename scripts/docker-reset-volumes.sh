#!/bin/bash

set -e

echo "Completely resetting Docker volumes for ArbiRich..."

# Stop all containers
docker compose down

# Remove volumes
docker volume rm arbirich_postgres_data || echo "Volume may not exist, continuing..."

# Prune any unused volumes
docker volume prune -f

echo "Docker volumes have been reset. You can now start with a fresh database."
