#!/bin/bash

set -e

host=${POSTGRES_HOST:-postgres}
port=${POSTGRES_PORT:-5432}
user=${POSTGRES_USER:-arbiuser}
password=${POSTGRES_PASSWORD:-postgres}
db=${POSTGRES_DB:-arbirich_db}
max_attempts=30
attempt=0

echo "Waiting for PostgreSQL to be ready..."
echo "Host: $host, Port: $port, User: $user, Database: $db"

# First check if PostgreSQL server is accessible via the user's default database
for default_db in "postgres" "$user" "template1"; do
  echo "Trying to connect using database: $default_db"
  
  if PGPASSWORD=$password psql -h "$host" -p "$port" -U "$user" -d "$default_db" -c "SELECT 1" > /dev/null 2>&1; then
    echo "Connected to PostgreSQL server using database: $default_db"
    system_db=$default_db
    break
  fi
  
  echo "Could not connect using database: $default_db"
done

if [ -z "$system_db" ]; then
  echo "Could not connect to PostgreSQL server using any known default database. Waiting..."
  
  # Try to connect with any known default database
  attempt=0
  while [ $attempt -lt $max_attempts ]; do
    attempt=$((attempt + 1))
    
    for default_db in "postgres" "$user" "template1"; do
      if PGPASSWORD=$password psql -h "$host" -p "$port" -U "$user" -d "$default_db" -c "SELECT 1" > /dev/null 2>&1; then
        echo "Connected to PostgreSQL server using database: $default_db"
        system_db=$default_db
        break 2
      fi
    done
    
    echo "PostgreSQL server not accessible yet. Waiting... ($attempt/$max_attempts)"
    sleep 1
  done
  
  if [ -z "$system_db" ]; then
    echo "PostgreSQL server is not accessible after $max_attempts attempts. Exiting."
    exit 1
  fi
fi

echo "PostgreSQL server is accessible using database: $system_db"

# Now check if our specific database exists
if ! PGPASSWORD=$password psql -h "$host" -p "$port" -U "$user" -d "$system_db" -c "SELECT 1 FROM pg_database WHERE datname = '$db';" | grep -q 1; then
  echo "Database $db does not exist. Creating it..."
  PGPASSWORD=$password psql -h "$host" -p "$port" -U "$user" -d "$system_db" -c "CREATE DATABASE $db WITH OWNER $user;"
  echo "Database $db created."
else
  echo "Database $db exists."
fi

# Finally check if we can connect to our database
until PGPASSWORD=$password psql -h "$host" -p "$port" -U "$user" -d "$db" -c "SELECT 1" > /dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ $attempt -ge $max_attempts ]; then
    echo "Could not connect to $db database after $max_attempts attempts. Exiting."
    exit 1
  fi
  echo "Cannot connect to $db database yet. Waiting... ($attempt/$max_attempts)"
  sleep 1
done

echo "PostgreSQL database $db is ready!"
