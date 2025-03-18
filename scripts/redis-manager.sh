#!/bin/bash

REDIS_CONTAINER="redis-arbirich"
REDIS_PORT=6379

function start_redis() {
    echo "Starting Redis..."
    if docker ps -a --filter "name=^/${REDIS_CONTAINER}$" | grep -q "${REDIS_CONTAINER}"; then
        echo "Redis container already exists. Ensuring it's running..."
        if ! docker ps --filter "name=^/${REDIS_CONTAINER}$" | grep -q "${REDIS_CONTAINER}"; then
            echo "Starting existing Redis container..."
            docker start ${REDIS_CONTAINER}
        else
            echo "Redis container is already running."
        fi
    else
        echo "Creating new Redis container..."
        docker run -d --name ${REDIS_CONTAINER} -p ${REDIS_PORT}:6379 redis:latest
    fi
}

function stop_redis() {
    echo "Stopping Redis..."
    if docker ps --filter "name=^/${REDIS_CONTAINER}$" | grep -q "${REDIS_CONTAINER}"; then
        echo "Stopping Redis container..."
        docker stop ${REDIS_CONTAINER}
    else
        echo "Redis container is not running."
    fi
}

function check_redis() {
    echo "Checking Redis status..."
    if docker ps --filter "name=^/${REDIS_CONTAINER}$" | grep -q "${REDIS_CONTAINER}"; then
        echo "Redis is running."
        return 0
    else
        echo "Redis is not running."
        return 1
    fi
}

function restart_redis() {
    echo "Restarting Redis..."
    stop_redis
    start_redis
}

function clean_redis() {
    echo "Cleaning up Redis..."
    stop_redis
    if docker ps -a --filter "name=^/${REDIS_CONTAINER}$" | grep -q "${REDIS_CONTAINER}"; then
        echo "Removing Redis container..."
        docker rm ${REDIS_CONTAINER}
    fi
}

function show_help() {
    echo "Redis Manager Script"
    echo "-------------------"
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  start    - Start Redis container"
    echo "  stop     - Stop Redis container"
    echo "  restart  - Restart Redis container"
    echo "  check    - Check if Redis is running"
    echo "  clean    - Stop and remove Redis container"
    echo "  help     - Show this help message"
}

# Process command line arguments
if [ $# -eq 0 ]; then
    show_help
    exit 1
fi

case "$1" in
    start)
        start_redis
        ;;
    stop)
        stop_redis
        ;;
    restart)
        restart_redis
        ;;
    check)
        check_redis
        ;;
    clean)
        clean_redis
        ;;
    help)
        show_help
        ;;
    *)
        echo "Unknown command: $1"
        show_help
        exit 1
        ;;
esac
