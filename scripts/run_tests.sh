#!/bin/bash

echo "🧪 Running E-Commerce Analytics Tests"
echo "===================================="

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Activate virtual environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run different test suites
echo -e "\n📋 Running Unit Tests..."
echo "------------------------"
pytest tests/unit -v --tb=short

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Unit tests passed${NC}"
else
    echo -e "${RED}❌ Unit tests failed${NC}"
    exit 1
fi

echo -e "\n📋 Running Integration Tests..."
echo "-------------------------------"
pytest tests/integration -v --tb=short

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Integration tests passed${NC}"
else
    echo -e "${RED}❌ Integration tests failed${NC}"
    exit 1
fi

echo -e "\n📊 Running Test Coverage..."
echo "---------------------------"
pytest --cov=src --cov-report=term-missing --cov-report=html

echo -e "\n${GREEN}✅ All tests completed!${NC}"
echo "Coverage report available in htmlcov/index.html"
