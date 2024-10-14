package com.fdu.msacs.dfs.metanode;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.fdu.msacs.dfs.metanode.meta.DfsNode;
import com.fdu.msacs.dfs.metanode.meta.DfsNode.HealthStatus;

@Service
public class NodeManager {
    private static final Logger logger = LoggerFactory.getLogger(NodeManager.class);
    
    private ConcurrentHashMap<String, DfsNode> registeredNodes;
    private ConcurrentHashMap<String, DfsNode> deadNodes;
    private int currentNodeIndex = 0;
    private ExecutorService executorService;
    private int roundRobinIndex = 0; // Index for round-robin selection

    @Value("${dfs.metanode.healthcheck.down_threshold:6}") 
    private int HEALTH_CHECK_THRESHOLD; 
    @Value("${dfs.metanode.healthcheck.warning_threshold:3}") 
    private int WARNING_THRESHOLD;

    public NodeManager() {
        this.registeredNodes = new ConcurrentHashMap<>();
        this.deadNodes = new ConcurrentHashMap<>();
        this.executorService = Executors.newFixedThreadPool(10);
    }

    public String registerNode(DfsNode node) {
        String nodeUrl = node.getContainerUrl();
        DfsNode existingNode = registeredNodes.get(nodeUrl);

        if (deadNodes.containsKey(nodeUrl)) {
            deadNodes.remove(nodeUrl);
            registeredNodes.put(nodeUrl, node);
            node.setHealthStatus(HealthStatus.HEALTHY);
            node.setLastTimeReport(new Date());
            logger.info("A dead node revives: {}", nodeUrl);
            return "A dead node revives: " + nodeUrl;
        }

        if (existingNode == null) {
            registeredNodes.put(nodeUrl, node);
            logger.info("A new node registered: {}", nodeUrl);
            return "Node registered: " + nodeUrl;
        } else {
            existingNode.setHealthStatus(HealthStatus.HEALTHY);
            existingNode.setLastTimeReport(new Date());
            logger.info("Node heartbeat listened from: {}", nodeUrl);
            return "Received Heartbeat from " + nodeUrl;
        }
    }
    
    public List<DfsNode> getReplicationNodes(String filename, String requestingNodeUrl) {
        logger.info("Retrieving replication nodes for filename: {} requested by: {}", filename, requestingNodeUrl);

        // Check for null inputs
        if (filename == null || requestingNodeUrl == null) {
            logger.error("Filename or requesting node URL is null.");
            return new ArrayList<>();  // Return an empty list to avoid null pointer exceptions
        }

        // Get all healthy nodes excluding the requesting node
        List<DfsNode> healthyNodes = registeredNodes.values().stream()
                .filter(node -> node != null && 
                                node.getHealthStatus() != null && 
                                node.getHealthStatus().equals(HealthStatus.HEALTHY) && 
                                !node.getContainerUrl().equals(requestingNodeUrl))
                .collect(Collectors.toList());

        // If there are no healthy nodes available, return an empty list
        if (healthyNodes.isEmpty()) {
            logger.warn("No healthy nodes available for replication for filename: {}", filename);
            return new ArrayList<>();
        }

        // If healthy nodes count is less than or equal to 2, return them directly
        if (healthyNodes.size() <= 2) {
            logger.info("Returning healthy nodes directly: {}", healthyNodes);
            return new ArrayList<>(healthyNodes);
        }

        // If there are more than 2 healthy nodes, perform round-robin selection
        List<DfsNode> selectedNodes = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            // Use the round-robin index to select nodes, checking for index validity
            int index = (roundRobinIndex + i) % healthyNodes.size();
            selectedNodes.add(healthyNodes.get(index));
            logger.debug("Selected node at index {}: {}", index, healthyNodes.get(index));
        }

        // Update the round-robin index for the next call
        roundRobinIndex = (roundRobinIndex + 2) % healthyNodes.size();
        logger.info("Updated round-robin index to: {}", roundRobinIndex);
        logger.info("Final selected replication nodes: {}", selectedNodes);

        return selectedNodes;
    }

    
    // Use Round-Robin to pick a node.
    public DfsNode selectNodeForUpload() {
        if (registeredNodes.isEmpty()) {
            return null;
        }
        List<DfsNode> nodeList = new ArrayList<>(registeredNodes.values());
        DfsNode selectedNode = nodeList.get(currentNodeIndex);
        currentNodeIndex = (currentNodeIndex + 1) % nodeList.size();
        return selectedNode;
    }

    public List<DfsNode> getRegisteredNodes() {
        return new ArrayList<>(registeredNodes.values());
    }
    
    public void clearRegisteredNodes() {
    	registeredNodes.clear();
    	deadNodes.clear();
    }

    @Scheduled(fixedRateString = "${dfs.metanode.healthcheck.rate:3000}")
    public void checkNodeHealth() {
        Date now = new Date();
        logger.info("Starting health check for registered nodes at {}.", now);

        registeredNodes.forEach((containerUrl, node) -> {
            long secondsSinceLastReport = (now.getTime() - node.getLastTimeReport().getTime()) / 1000;

            if (secondsSinceLastReport > HEALTH_CHECK_THRESHOLD) {
                logger.info("The node({}) is down. Moving to deadNodes from registered nodes.", node.getContainerUrl());
                deadNodes.put(node.getContainerUrl(), node);
                registeredNodes.remove(containerUrl);
                executorService.submit(() -> handleDeadNode(node));
            } else if (secondsSinceLastReport > WARNING_THRESHOLD) {
                node.updateHealthStatus(HealthStatus.WARNING);
                logger.info("Warning: node({}) has not reported for {} seconds. Status updated to WARNING.",
                            node.getContainerUrl(), secondsSinceLastReport);
            } else {
                node.updateHealthStatus(HealthStatus.HEALTHY);
                logger.info("Node({}) is healthy. Status remains HEALTHY.", node.getContainerUrl());
            }
        });

        logger.info("Health check completed at {}. Total nodes monitored: {}.", now, registeredNodes.size());
    }

    private void handleDeadNode(DfsNode deadNode) {
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Additional logic for handling the dead node can go here.
    }

	public Map<String, DfsNode> getDeadNodes() {
        return deadNodes;
	}
}
