@echo off
echo "Building Docker image..."
docker build -t dfs-meta-svr .

echo "Stopping and Removing existing containers..."
docker stop dfs-meta-svr
docker rm dfs-meta-svr

echo "Deploying new containers..."
docker run -d -p 8080:8080 --name dfs-meta-svr dfs-meta-svr

echo "Deployment completed!"
