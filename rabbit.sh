#!/bin/bash

# the bash function for starting/running the docker container
start_or_run () {
    # inspect docker command that is piped into nothing to hide it at channel two which is the std error into channel one which is already pointing to a black hole
    docker inspect peril_rabbitmq > /dev/null 2>&1

    # the if statement checks if the last command ran successfully, if yes, it just starts the already there container
    if [ $? -eq 0 ]; then
        echo "Starting Peril RabbitMQ container..."
        docker start peril_rabbitmq
    # if not runs a fresh container with proper arguments
    else
        echo "Peril RabbitMQ container not found, creating a new one..."
        docker run -d --name peril_rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
    fi
}

# this acts like a switch statement, looks for the second option cmd after our executable, options like start, stop, logs, if none, tells how to use
case "$1" in
    start)
        start_or_run
        ;;
    stop)
        echo "Stopping Peril RabbitMQ container..."
        docker stop peril_rabbitmq
        ;;
    logs)
        echo "Fetching logs for Peril RabbitMQ container..."
        docker logs -f peril_rabbitmq
        ;;
    *)
        echo "Usage: $0 {start|stop|logs}"
        exit 1
esac
