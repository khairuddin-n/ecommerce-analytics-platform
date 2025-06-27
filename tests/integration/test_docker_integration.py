"""Integration tests for Docker setup"""

import pytest
import subprocess
import time
import requests
from pathlib import Path
import docker
import os


@pytest.mark.skipif(not os.environ.get('RUN_DOCKER_INTEGRATION'), 
                    reason="Skip Docker integration tests unless RUN_DOCKER_INTEGRATION is set")
class TestDockerIntegration:
    """Test Docker services integration"""
    
    @pytest.fixture(scope="class")
    def docker_client(self):
        """Get Docker client"""
        try:
            client = docker.from_env()
            return client
        except:
            pytest.skip("Docker not available")
    
    @pytest.fixture(scope="class", autouse=True)
    def docker_services(self, docker_client):
        """Start Docker services for tests"""
        project_root = Path(__file__).parent.parent.parent
        
        # Start services
        subprocess.run(
            ["docker-compose", "up", "-d", "postgres", "analytics"],
            cwd=project_root,
            check=True
        )
        
        # Wait for services to be ready
        time.sleep(10)
        
        yield
        
        # Cleanup
        subprocess.run(
            ["docker-compose", "down"],
            cwd=project_root
        )
    
    def test_postgres_connection(self, docker_client):
        """Test PostgreSQL container is running"""
        containers = docker_client.containers.list()
        postgres_running = any('postgres' in c.name for c in containers)
        assert postgres_running, "PostgreSQL container not running"
    
    def test_analytics_container_starts(self, docker_client):
        """Test analytics container can start"""
        containers = docker_client.containers.list(all=True)
        analytics_exists = any('analytics' in c.name for c in containers)
        assert analytics_exists, "Analytics container not found"
    
    def test_data_volume_mount(self):
        """Test data volumes are properly mounted"""
        result = subprocess.run(
            ["docker-compose", "run", "--rm", "analytics", "ls", "-la", "/app/data"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0
        assert "raw" in result.stdout
        assert "processed" in result.stdout
    
    def test_python_imports_in_container(self):
        """Test Python imports work in container"""
        result = subprocess.run(
            ["docker-compose", "run", "--rm", "analytics", 
             "python", "-c", "from src.pipeline.main import EcommerceAnalyticsPipeline; print('Import successful')"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0
        assert "Import successful" in result.stdout