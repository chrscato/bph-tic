#!/bin/bash

# Set environment variables
export PAYER1_USERNAME="your_username"
export PAYER1_PASSWORD="your_password"
export PAYER2_TOKEN="your_token"

# Create output directory
mkdir -p output

# Run the scraper
python -m tic_mrf_scraper \
    --config config.yaml \
    --output output \
    --upload 