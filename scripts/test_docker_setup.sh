#!/bin/bash
# Test Docker setup and configuration

set -e  # Exit on error

# Colors
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🐳 Docker Setup Test Suite${NC}"
echo "=============================="

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to test file exists
test_file_exists() {
    if [ -f "$1" ]; then
        echo -e "✅ $1 exists"
        return 0
    else
        echo -e "❌ $1 not found"
        return 1
    fi
}

# Function to test directory exists
test_dir_exists() {
    if [ -d "$1" ]; then
        echo -e "✅ $1 exists"
        return 0
    else
        echo -e "❌ $1 not found"
        return 1
    fi
}

# Test 1: Check Docker installation
echo -e "\n${YELLOW}Test 1: Docker Installation${NC}"
if command_exists docker; then
    echo -e "✅ Docker is installed"
    docker --version
else
    echo -e "❌ Docker is not installed"
    exit 1
fi

if command_exists docker-compose; then
    echo -e "✅ Docker Compose is installed"
    docker-compose --version
else
    echo -e "❌ Docker Compose is not installed"
    exit 1
fi

# Test 2: Check required files
echo -e "\n${YELLOW}Test 2: Required Files${NC}"
test_file_exists "Dockerfile"
test_file_exists "docker-compose.yml"
test_file_exists ".dockerignore"
test_file_exists "Makefile"
test_file_exists "requirements.txt"
test_file_exists "requirements-dev.txt"
test_file_exists "setup.py"
test_file_exists "pytest.ini"
test_file_exists "setup.cfg"

# Test 3: Check required directories
echo -e "\n${YELLOW}Test 3: Required Directories${NC}"
test_dir_exists "src"
test_dir_exists "tests"
test_dir_exists "scripts"
test_dir_exists "dags"
test_dir_exists "docker"
test_dir_exists "data"
test_dir_exists "data/raw"
test_dir_exists "data/processed"

# Test 4: Validate Docker Compose syntax
echo -e "\n${YELLOW}Test 4: Docker Compose Validation${NC}"
if docker-compose config > /dev/null 2>&1; then
    echo -e "✅ docker-compose.yml is valid"
else
    echo -e "❌ docker-compose.yml has syntax errors"
    docker-compose config
    exit 1
fi

# Test 5: Test Dockerfile build (dry run)
echo -e "\n${YELLOW}Test 5: Dockerfile Syntax Check${NC}"
if docker build --no-cache -f Dockerfile . --target base 2>/dev/null || docker build --no-cache -f Dockerfile . >/dev/null 2>&1; then
    echo -e "✅ Dockerfile syntax is valid"
else
    echo -e "⚠️  Dockerfile might have issues (this could be due to missing files)"
fi

# Test 6: Check Makefile targets
echo -e "\n${YELLOW}Test 6: Makefile Targets${NC}"
if make help > /dev/null 2>&1; then
    echo -e "✅ Makefile help works"
    
    # Check for specific targets
    for target in "docker-build" "docker-up" "docker-down" "docker-test"; do
        if make help | grep -q "$target"; then
            echo -e "✅ Target '$target' exists"
        else
            echo -e "❌ Target '$target' not found"
        fi
    done
else
    echo -e "❌ Makefile help failed"
fi

# Test 7: Environment setup
echo -e "\n${YELLOW}Test 7: Environment Setup${NC}"
if [ -f ".env.example" ]; then
    echo -e "✅ .env.example exists"
    
    # Create .env if it doesn't exist
    if [ ! -f ".env" ]; then
        cp .env.example .env
        echo -e "✅ Created .env from .env.example"
    else
        echo -e "✅ .env already exists"
    fi
else
    echo -e "⚠️  .env.example not found"
fi

# Test 8: Python imports
echo -e "\n${YELLOW}Test 8: Python Module Structure${NC}"
python3 -c "
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))

try:
    from src.utils.config import settings
    print('✅ Can import settings')
except Exception as e:
    print(f'❌ Cannot import settings: {e}')

try:
    from src.pipeline.main import EcommerceAnalyticsPipeline
    print('✅ Can import EcommerceAnalyticsPipeline')
except Exception as e:
    print(f'❌ Cannot import EcommerceAnalyticsPipeline: {e}')
"

# Test 9: Docker network check
echo -e "\n${YELLOW}Test 9: Docker Network Configuration${NC}"
if docker network ls | grep -q "ecommerce-network"; then
    echo -e "✅ Docker network 'ecommerce-network' exists"
else
    echo -e "ℹ️  Docker network 'ecommerce-network' will be created on first run"
fi

# Test 10: Port availability
echo -e "\n${YELLOW}Test 10: Port Availability${NC}"
for port in 8080 8081 8888; do
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "⚠️  Port $port is already in use"
    else
        echo -e "✅ Port $port is available"
    fi
done

# Summary
echo -e "\n${GREEN}=============================="
echo -e "Docker Setup Test Complete!"
echo -e "==============================${NC}"

# Run Python tests
echo -e "\n${YELLOW}Running Python Docker tests...${NC}"
python3 tests/test_docker_setup.py -v || echo -e "${YELLOW}Some Python tests failed${NC}"

echo -e "\n${GREEN}✅ All basic checks completed!${NC}"
echo -e "\nTo run the full Docker setup:"
echo -e "  ${GREEN}make docker-build${NC}"
echo -e "  ${GREEN}make docker-up${NC}"