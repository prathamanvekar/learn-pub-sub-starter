#!/bin/bash

# Check if the number of instances was provided
# the if condition checks if the string was empty, -z is a unary operator that returns true if the string that follows it has len of zero
# $1 specifies the 1st argument and the "" around the $1 catches edge cases like whitespaces or missing spaces
if [ -z "$1" ]; then
  echo "Usage: $0 <number-of-instances>"
  exit 1
fi

# saves the number of instances the user inputs
num_instances=$1

# Array to store process IDs
# the -a attribute flag creates a 0-indexed array, in contrast with -A that creates the associative array with key-value structure
declare -a pids

# Function to kill all processes when Ctrl+C is pressed
cleanup() {
  echo "Terminating all instances of ./cmd/server..."
  # "${pids[@]}" $ the expansion operator | @ opens the array into distinct words 
  for pid in "${pids[@]}"; do
    # the kill sends a signal toe sigterm(signal 15) asking the process to gracefully shutdown
    kill -SIGTERM "$pid"
  done
  exit
}

# Setup trap for SIGINT
# trap is a built in command that sys signals and redirects them to proper actions
# SIGINT: Short for Signal Interrupt (POSIX Signal 2), the standard signal sent by the terminal when a user presses Ctrl+C.
trap 'cleanup' SIGINT

# Start the specified number of instances of the program in the background
for (( i=0; i<num_instances; i++ )); do
  # &: The background operator. Sends it the background without blocking  
  go run ./cmd/server &
  # $!: A special Bash variable containing the Process ID (PID) of the most recent background job launched by this shell
  # (...): Array initialization syntax used when adding elements to Bash arrays.
  pids+=($!)
done

# Wait for all background processes to finish
wait
