#!/bin/bash
# Reset local PostgreSQL database and rerun migrations
set -e

# Load environment variables if .env file exists
if [ -f .env ]; then
    echo "Loading environment variables from .env file"
    source .env
fi

# Set default values for database connection if not set in environment
DB_HOST=${POSTGRES_HOST:-localhost}
DB_PORT=${POSTGRES_PORT:-5432}
DB_NAME=${POSTGRES_DB:-arbirich_db}
DB_USER=${POSTGRES_USER:-arbiuser}
DB_PASSWORD=${POSTGRES_PASSWORD:-postgres}

# Text colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== ArbiRich Local Database Reset Script ===${NC}"
echo "This script will DROP and recreate your local database!"
echo "Database: $DB_NAME on $DB_HOST:$DB_PORT"
echo -e "${RED}WARNING: All data will be lost!${NC}"
echo

# Prompt for confirmation
read -p "Are you sure you want to continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Operation canceled."
    exit 1
fi

echo -e "\n${YELLOW}Connecting to PostgreSQL...${NC}"

# Test connection 
if ! PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "SELECT 1" >/dev/null 2>&1; then
    echo -e "${RED}Error: Could not connect to PostgreSQL. Check connection settings.${NC}"
    echo "Make sure PostgreSQL is running and the connection details are correct:"
    echo "Host: $DB_HOST"
    echo "Port: $DB_PORT"
    echo "User: $DB_USER"
    echo "Database: postgres (for initial connection)"
    exit 1
fi

echo -e "${GREEN}Connection successful!${NC}"

# Drop and recreate database
echo -e "\n${YELLOW}Dropping database $DB_NAME if it exists...${NC}"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME;"

echo -e "${YELLOW}Creating new database $DB_NAME...${NC}"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "CREATE DATABASE $DB_NAME WITH OWNER $DB_USER;"

echo -e "${GREEN}Database reset complete!${NC}"

# Create the basic tables first
echo -e "\n${YELLOW}Running migrations to create schema...${NC}"
cd "$(dirname "$0")/.." # Change to project root directory

# Run migrations using Alembic
echo -e "${YELLOW}Checking for alembic migrations...${NC}"
if [ -d "alembic/versions" ]; then
    echo -e "${GREEN}Found alembic migrations directory!${NC}"

    # Check if we're using Poetry or standard pip
    if command -v poetry &> /dev/null && [ -f "pyproject.toml" ]; then
        echo "Using Poetry to run alembic migrations"
        poetry run alembic upgrade head
    elif command -v alembic &> /dev/null; then
        echo "Using system Alembic"
        alembic upgrade head
    else
        echo "Running with Python directly"
        python -m alembic upgrade head
    fi
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Alembic migrations applied successfully!${NC}"
    else
        echo -e "${RED}Alembic migrations failed!${NC}"
        echo "Attempting to run manual migrations..."
        
        # Fall back to running migration scripts directly
        run_manual_migrations
    fi
else
    echo -e "${YELLOW}Alembic migrations directory not found.${NC}"
    run_manual_migrations
fi

# Function to run manual migrations if alembic fails
run_manual_migrations() {
    echo -e "${YELLOW}Running manual migrations...${NC}"
    
    # First try running our migration scripts directly
    if [ -d "migrations" ]; then
        for migration_file in migrations/add_*.py; do
            if [ -f "$migration_file" ]; then
                echo "Running migration script: $migration_file"
                python "$migration_file"
            fi
        done
    else
        echo -e "${YELLOW}No migrations directory found. Checking for individual script.${NC}"
        
        # Then check for specific migration script
        if [ -f "src/arbirich/models/schema.py" ]; then
            echo "Running schema creation from models/schema.py"
            python -c "
import sys
sys.path.append('.')
from src.arbirich.models.schema import metadata
from src.arbirich.config.config import DATABASE_URL
from sqlalchemy import create_engine
engine = create_engine(DATABASE_URL)
metadata.create_all(engine)
print('Schema created successfully from schema.py!')
"
        else
            echo -e "${RED}No migration sources found!${NC}"
            echo "Please check that you have either:"
            echo "1. alembic/versions/ directory with migration files"
            echo "2. migrations/ directory with Python scripts"
            echo "3. src/arbirich/models/schema.py file"
            exit 1
        fi
    fi
}

echo -e "\n${GREEN}Database schema setup complete!${NC}"

# Optional: Create seed data if requested
read -p "Do you want to create seed data? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "\n${YELLOW}Creating seed data...${NC}"
    
    # Run seed script if it exists
    if [ -f "scripts/create_seed_data.py" ]; then
        # Check if Poetry is available and use it if so
        if command -v poetry &> /dev/null && [ -f "pyproject.toml" ]; then
            echo "Using Poetry to run seed script"
            poetry run python scripts/create_seed_data.py
        else
            echo "Running seed script with system Python"
            python scripts/create_seed_data.py
        fi
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Seed data created successfully!${NC}"
        else
            echo -e "${RED}Error creating seed data!${NC}"
            echo "Try running: poetry run python scripts/create_seed_data.py"
        fi
    else
        echo -e "${YELLOW}Seed script not found. Skipping.${NC}"
    fi
fi

echo -e "\n${GREEN}Database setup complete!${NC}"
