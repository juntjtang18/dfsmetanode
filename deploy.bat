@echo off
setlocal

set NETWORK_NAME=mybridge
set CONTAINER_NAME=dfs-meta-node

echo "Building Docker image..."
docker build -t %CONTAINER_NAME% .

echo "Checking if the network '%NETWORK_NAME%' exists..."
docker network inspect %NETWORK_NAME% >nul 2>&1

if %errorlevel% neq 0 (
    echo "Network '%NETWORK_NAME%' does not exist. Creating it..."
    docker network create %NETWORK_NAME%
) else (
    echo "Network '%NETWORK_NAME%' already exists."
)

echo "Stopping and removing existing containers..."
docker stop %CONTAINER_NAME% >nul 2>&1
docker rm %CONTAINER_NAME% >nul 2>&1

echo "Deploying new container..."
docker run -d --network %NETWORK_NAME% -p 8080:8080 --name %CONTAINER_NAME% %CONTAINER_NAME%

echo "Deployment completed!"
