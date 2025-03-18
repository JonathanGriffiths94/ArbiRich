#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

function print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}\n"
}

function print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

function print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

function print_error() {
    echo -e "${RED}✗ $1${NC}"
}

function check_docker() {
    print_header "Checking Docker installation"
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    print_success "Docker is installed"

    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running. Please start Docker."
        exit 1
    fi
    print_success "Docker daemon is running"
}

function start_services() {
    print_header "Starting ArbiRich services"
    
    # Build the images
    echo "Building Docker images..."
    docker compose build || { print_error "Failed to build Docker images"; exit 1; }
    
    # Start PostgreSQL and Redis first
    echo "Starting PostgreSQL and Redis..."
    docker compose up -d postgres redis || { print_error "Failed to start PostgreSQL and Redis"; exit 1; }
    
    # Wait for PostgreSQL to be ready
    echo "Waiting for PostgreSQL to be ready..."
    until docker compose exec postgres pg_isready -U arbiuser -d arbirich_db > /dev/null 2>&1; do
        echo "PostgreSQL is still unavailable - sleeping for 1 second"
        sleep 1
    done
    print_success "PostgreSQL is ready"
    
    # Run migrations
    echo "Running database migrations..."
    docker compose up migrate || { print_warning "Migrations may not have completed successfully"; }
    
    # Start the application
    echo "Starting the application..."
    docker compose up -d app || { print_error "Failed to start application"; exit 1; }
    
    print_success "All services are up and running!"
    echo -e "\nYou can now access the API at: http://localhost:8080"
    echo "Monitor logs with: docker compose logs -f app"
}

function stop_services() {
    print_header "Stopping ArbiRich services"
    docker compose down
    print_success "All services stopped"
}

function show_logs() {
    print_header "Showing logs for ArbiRich services"
    docker compose logs -f
}

function reset_database() {
    print_header "Resetting ArbiRich database"
    
    # Stop all services
    docker compose down
    
    # Remove the volume
    docker volume rm arbirich_postgres_data || true
    
    # Start services again
    start_services
    
    print_success "Database has been reset"
}

function show_help() {
    echo -e "${BLUE}ArbiRich Docker Helper${NC}"
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  start      Start all services"
    echo "  stop       Stop all services"
    echo "  logs       Show logs from all services"
    echo "  reset-db   Reset the database and restart services"
    echo "  help       Show this help message"
}

# Main script logic
check_docker

case "$1" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    logs)
        show_logs
        ;;
    reset-db)
        reset_database
        ;;
    help|*)
        show_help
        ;;
esac
