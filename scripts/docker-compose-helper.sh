#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
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

function display_banner() {
    clear
    echo -e "${CYAN}"
    echo '$$$$$$\            $$\       $$\ $$$$$$$\  $$\           $$\       '
    echo '$$  __$$\           $$ |      \__|$$  __$$\ \__|          $$ |      '
    echo '$$ /  $$ | $$$$$$\  $$$$$$$\  $$\ $$ |  $$ |$$\  $$$$$$$\ $$$$$$$\  '
    echo '$$$$$$$$ |$$  __$$\ $$  __$$\ $$ |$$$$$$$  |$$ |$$  _____|$$  __$$\ '
    echo '$$  __$$ |$$ |  \__|$$ |  $$ |$$ |$$  __$$< $$ |$$ /      $$ |  $$ |'
    echo '$$ |  $$ |$$ |      $$ |  $$ |$$ |$$ |  $$ |$$ |$$ |      $$ |  $$ |'
    echo '$$ |  $$ |$$ |      $$$$$$$  |$$ |$$ |  $$ |$$ |\$$$$$$$\ $$ |  $$ |'
    echo '\__|  \__|\__|      \_______/ \__|\__|  \__|\__| \_______|\__|  \__|'
    echo -e "${NC}"
    
    # Pause for 5 seconds after showing the logo
    sleep 5
    
    # Show platform and environment info
    echo -e "${GREEN}Cryptocurrency Arbitrage Platform${NC}"

    sleep 3
    
    echo -e "${YELLOW}Docker Environment Management${NC}"
    echo ""
    
    # Pause for 3 seconds before continuing
    sleep 3
}

function display_start_sequence() {
    echo -e "${YELLOW}Initializing docker environment...${NC}"
    
    # Short startup animation
    for i in {1..3}; do
        printf "."
        sleep 0.3
    done
    echo -e "\n"
    
    # Display configuration info
    echo -e "${CYAN}Configuration:${NC}"
    echo -e "  ${WHITE}PostgreSQL:${NC} postgres:5432/arbirich_db"
    echo -e "  ${WHITE}Redis:${NC}      redis:6379"
    echo -e "  ${WHITE}Web API:${NC}    http://localhost:8080"
    echo ""
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
    # Display the ASCII art banner
    display_banner
    
    # Show start sequence animation
    display_start_sequence
    
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
    
    # Final status display
    echo -e "\n${GREEN}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║${NC}  ArbiRich is now online and ready to use!     ${GREEN}║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "Web API:     ${CYAN}http://localhost:8080${NC}"
    echo -e "API Docs:    ${CYAN}http://localhost:8080/docs${NC}"
    echo -e "Monitor logs: ${WHITE}just docker-logs${NC}"
    echo -e "Stop services: ${WHITE}just docker-stop${NC}"
    echo ""
}

function stop_services() {
    # Display the banner when stopping too
    display_banner
    
    print_header "Stopping ArbiRich services"
    
    echo -e "${YELLOW}Preparing for shutdown...${NC}"
    
    # Show a short countdown animation
    echo -ne "${RED}Shutting down in 3...${NC}"
    sleep 0.7
    echo -ne "\r${RED}Shutting down in 2...${NC}"
    sleep 0.7
    echo -ne "\r${RED}Shutting down in 1...${NC}"
    sleep 0.7
    echo -e "\r${RED}Initiating shutdown...   ${NC}"
    
    # Show what's being stopped
    echo ""
    echo -e "${CYAN}Stopping containers:${NC}"
    docker compose ps --services | while read -r service; do
        echo -e "  - ${YELLOW}$service${NC}"
    done
    echo ""
    
    # Actually stop the services
    echo -e "${YELLOW}Shutting down containers...${NC}"
    docker compose down
    
    # Use the same banner for shutdown for consistency
    echo ""
    echo -e "${RED}"
    echo '$$$$$$\            $$\       $$\ $$$$$$$\  $$\           $$\       '
    echo '$$  __$$\           $$ |      \__|$$  __$$\ \__|          $$ |      '
    echo '$$ /  $$ | $$$$$$\  $$$$$$$\  $$\ $$ |  $$ |$$\  $$$$$$$\ $$$$$$$\  '
    echo '$$$$$$$$ |$$  __$$\ $$  __$$\ $$ |$$$$$$$  |$$ |$$  _____|$$  __$$\ '
    echo '$$  __$$ |$$ |  \__|$$ |  $$ |$$ |$$  __$$< $$ |$$ /      $$ |  $$ |'
    echo '$$ |  $$ |$$ |      $$ |  $$ |$$ |$$ |  $$ |$$ |$$ |      $$ |  $$ |'
    echo '$$ |  $$ |$$ |      $$$$$$$  |$$ |$$ |  $$ |$$ |\$$$$$$$\ $$ |  $$ |'
    echo '\__|  \__|\__|      \_______/ \__|\__|  \__|\__| \_______|\__|  \__|'
    echo -e "${NC}"
    echo ""
    
    # Show a simple text indication that this is shutdown, not startup
    echo -e "${RED}[SYSTEM SHUTDOWN COMPLETE]${NC}"
    echo ""
    
    # Show confirmation message
    print_success "All services stopped"
    echo -e "\n${GREEN}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║${NC}  ArbiRich services have been shut down safely.  ${GREEN}║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "To restart services: ${WHITE}just docker-start${NC}"
    echo -e "To clean up completely: ${WHITE}docker volume rm arbirich_postgres_data${NC}"
    echo ""
}

function show_logs() {
    print_header "Showing logs for ArbiRich services"
    echo -e "${YELLOW}Press Ctrl+C to exit logs viewer${NC}\n"
    sleep 1
    docker compose logs -f
}

function reset_database() {
    print_header "Resetting ArbiRich database"
    
    echo -e "${YELLOW}This will delete all data in the database. Are you sure? (y/N)${NC}"
    read -r confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_warning "Operation cancelled"
        return
    fi
    
    # Stop all services
    echo "Stopping all services..."
    docker compose down
    
    # Remove the volume
    echo "Removing database volume..."
    docker volume rm arbirich_postgres_data || true
    
    print_success "Database volume removed"
    
    # Start services again
    echo "Restarting services with fresh database..."
    start_services
    
    print_success "Database has been reset"
}

function show_help() {
    display_banner
    
    echo -e "${WHITE}ArbiRich Docker Helper${NC}"
    echo -e "${CYAN}────────────────────────────${NC}"
    echo "Usage: $0 [command]"
    echo ""
    echo -e "${WHITE}Commands:${NC}"
    echo -e "  ${GREEN}start${NC}      Start all services"
    echo -e "  ${YELLOW}stop${NC}       Stop all services"
    echo -e "  ${BLUE}logs${NC}       Show logs from all services"
    echo -e "  ${RED}reset-db${NC}   Reset the database and restart services"
    echo -e "  ${CYAN}help${NC}       Show this help message"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo "  ./scripts/docker-compose-helper.sh start"
    echo "  just docker-start"
    echo ""
}

# Main script logic
if [[ "$1" != "help" ]]; then
    check_docker
fi

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
