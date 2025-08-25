#!/bin/bash

# API endpoints
API_URL="http://localhost:8000/api/people"
EXTERNAL_API_URL="http://localhost:8000/api/external/people"
ERROR_API_URL="http://localhost:8000/api/people/5000"  # Added error endpoint

# Function to generate random name
random_name() {
    NAMES=("Alice" "Bob" "Charlie" "Dave" "Eve" "Frank" "Grace" "Hannah")
    echo "${NAMES[$RANDOM % ${#NAMES[@]}]}"
}

# Function to generate random age (between 18 and 80)
random_age() {
    echo $((18 + $RANDOM % 63))
}

# Store the start time
START_TIME=$(date +%s)

# Infinite loop to call the APIs every 5 seconds
while true; do
    # Calculate elapsed time in seconds
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))

    # Check if 60 seconds have passed
    if [ "$ELAPSED_TIME" -ge 60 ]; then
        echo "Deleting all data..."
        curl --location --request DELETE "$API_URL" \
             --header 'Content-Type: application/json'
        # Reset start time after deletion
        START_TIME=$(date +%s)
    fi

    # Randomly choose between GET (0), POST (1), EXTERNAL GET (2), and ERROR GET (3)
    ACTION=$((RANDOM % 4))  # Increased to 4 options to include error case

    if [ "$ACTION" -eq 0 ]; then
        # GET request to main API
        echo "Sending GET request to main API..."
        curl --location --request GET "$API_URL" \
             --header 'Content-Type: application/json'
    elif [ "$ACTION" -eq 1 ]; then
        # POST request to main API with random data
        NAME=$(random_name)
        AGE=$(random_age)
        echo "Sending POST request with name: $NAME, age: $AGE..."
        curl --location --request POST "$API_URL" \
             --header 'Content-Type: application/json' \
             --data "{\"name\": \"$NAME\", \"age\": $AGE}"
    elif [ "$ACTION" -eq 2 ]; then
        # GET request to external API
        echo "Sending GET request to external API..."
        curl --location --request GET "$EXTERNAL_API_URL" \
             --header 'Content-Type: application/json'
    else
        # GET request to error-inducing endpoint
        echo "Sending GET request to error endpoint (should fail)..."
        curl --location --request GET "$ERROR_API_URL" \
             --header 'Content-Type: application/json'
    fi

    # Wait 5 seconds before the next request
    sleep 5
done
