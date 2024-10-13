@echo off
setlocal

set NETWORK_NAME=mybridge
set META_NODE_NAME=dfs-meta-node
set MONGODB_NAME=dfs-mongodb
set MONGODB_PORT=27017
set MONGODB_VOLUME=mongodb-data  
set MONGODB_LOCAL_NAME=mongodb  

echo "Checking if the network '%NETWORK_NAME%' exists..."
docker network inspect %NETWORK_NAME% >nul 2>&1

if %errorlevel% neq 0 (
    echo "Network '%NETWORK_NAME%' does not exist. Creating it..."
    docker network create %NETWORK_NAME%
) else (
    echo "Network '%NETWORK_NAME%' already exists."
)

echo "Stopping and removing existing containers..."
docker stop %META_NODE_NAME% >nul 2>&1
docker rm %META_NODE_NAME% >nul 2>&1

REM echo "Deploying local MongoDB container (accessible at localhost:27017)..."
REM docker run --name mongodb -d -p 27017:27017 -v mongodata:/data/db mongo

REM echo "Deploying MongoDB container (accessible by container name dfs-mongodb)..."
REM docker run -d --network %NETWORK_NAME% --name %MONGODB_NAME% -p 27017:27017 -v mongodb-data:/data/db mongo:latest

echo "Building Docker image for the DFS Meta Node..."
docker build -t %META_NODE_NAME% .

echo "Deploying new DFS Meta Node container..."
docker run -d --network %NETWORK_NAME% -p 8080:8080 --name %META_NODE_NAME% %META_NODE_NAME%

echo "Deployment completed!"
