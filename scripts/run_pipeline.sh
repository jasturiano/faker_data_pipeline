#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🚀 Starting Pipeline...${NC}"

# Array of steps to execute
steps=("clean" "quality" "test" "fetch-data" "reports")
step_names=("Cleaning" "Quality Checks" "Tests" "Data Fetch" "Reports")

for i in "${!steps[@]}"; do
    echo -e "\n${BLUE}Running: ${step_names[$i]}...${NC}"
    if ! make ${steps[$i]} GENDER=${GENDER:-male} BATCH_SIZE=${BATCH_SIZE:-1000}; then
        echo -e "\n${RED}💥 Pipeline Failed at: ${step_names[$i]}${NC}"
        exit 1
    fi
done

echo -e "\n${GREEN}✅ Pipeline completed successfully!${NC}" 