#!/bin/bash
set -e

# Wait for the database to be ready
echo "Waiting for database to be ready..."
until PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 1" > /dev/null 2>&1; do
  echo "Database is unavailable - sleeping"
  sleep 1
done
echo "Database is up and running!"

# Run migrations
if [ "$1" = "migrate" ]; then
  echo "Running database migrations..."
  python -m alembic upgrade head
  echo "Migrations completed successfully!"
  exit 0
fi

# Run the application
if [ "$1" = "app" ]; then
  echo "Starting ArbiRich application..."
  python -m src.arbirich.main
  exit 0
fi

# If a command is passed, execute it
exec "$@"
