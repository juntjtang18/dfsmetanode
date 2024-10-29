package com.infolink.dfs.metanode;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.infolink.dfs.metanode.BlockMetaController.ResponseNodesForBlock;
import com.infolink.dfs.shared.DfsNode;

@Service
public class NodeManager {
    private static final Logger logger = LoggerFactory.getLogger(NodeManager.class);
    
    private ConcurrentHashMap<String, DfsNode> registeredNodes;
    private ConcurrentHashMap<String, DfsNode> deadNodes;
    private int currentNodeIndex = 0;
    private ExecutorService executorService;
    private int roundRobinIndex = 0;

    @Value("${dfs.metanode.healthcheck.down_threshold:6}") 
    private int HEALTH_CHECK_THRESHOLD; 

    public NodeManager() {
        this.registeredNodes = new ConcurrentHashMap<>();
        this.deadNodes = new ConcurrentHashMap<>();
        this.executorService = Executors.newFixedThreadPool(10);
    }

    public String registerNode(DfsNode node) {
        String nodeUrl = node.getContainerUrl();
        DfsNode existingNode = registeredNodes.get(nodeUrl);
        String returnMsg = "";
        
        if (deadNodes.containsKey(nodeUrl)) {
            deadNodes.remove(nodeUrl);
            registeredNodes.put(nodeUrl, node);
            node.setLastTimeReport(new Date());
            logger.debug("A dead node revives: {}", nodeUrl);
            returnMsg = "A dead node revives: " + nodeUrl;
        }

        if (existingNode == null) {
            registeredNodes.put(nodeUrl, node);
            //logger.debug("A new node registered: {}", nodeUrl);
            returnMsg = "Node registered: " + nodeUrl;
        } else {
            existingNode.setLastTimeReport(new Date());
            //logger.debug("Node heartbeat listened from: {}", nodeUrl);
            returnMsg = "Received Heartbeat from " + nodeUrl;
        }
        // Log all registered nodes at debug level after method execution, including revived nodes
        //logger.debug("All registered nodes after registration: {}", new ArrayList<>(registeredNodes.values()));
        return returnMsg;
    }
    
    public List<DfsNode> getReplicationNodes(String filename, String requestingNodeUrl) {
        logger.info("Retrieving replication nodes for filename: {} requested by: {}", filename, requestingNodeUrl);

        // Validate input parameters
        if (filename == null || requestingNodeUrl == null) {
            logger.error("Filename or requesting node URL is null.");
            return new ArrayList<>();
        }

        // Get live nodes excluding the requesting node
        List<DfsNode> liveNodes = registeredNodes.values().stream()
                .filter(node -> !node.getContainerUrl().equals(requestingNodeUrl))
                .collect(Collectors.toList());

        // If there are no other live nodes, log a warning and return an empty list
        if (liveNodes.isEmpty()) {
            logger.warn("No other live nodes available for replication for filename: {}", filename);
            return new ArrayList<>();
        }

        // If there are 1 or 2 live nodes, return them directly
        if (liveNodes.size() <= 2) {
            logger.info("Returning available live nodes directly: {}", liveNodes);
            return new ArrayList<>(liveNodes);
        }

        // Select up to 2 nodes using round-robin logic
        List<DfsNode> selectedNodes = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            int index = (roundRobinIndex + i) % liveNodes.size();
            selectedNodes.add(liveNodes.get(index));
            logger.debug("Selected node at index {}: {}", index, liveNodes.get(index));
        }

        // Update the round-robin index for the next selection
        roundRobinIndex = (roundRobinIndex + 2) % liveNodes.size();
        logger.info("Updated round-robin index to: {}", roundRobinIndex);
        logger.info("Final selected replication nodes: {}", selectedNodes);

        return selectedNodes;
    }

    public DfsNode getNodeByContainerUrl(String containerUrl) {
        return registeredNodes.get(containerUrl);
    }
    
    public DfsNode selectNodeRoundRobin() {
        if (registeredNodes.isEmpty()) {
            return null;
        }
        List<DfsNode> nodeList = new ArrayList<>(registeredNodes.values());
        DfsNode selectedNode = nodeList.get(currentNodeIndex);
        currentNodeIndex = (currentNodeIndex + 1) % nodeList.size();
        return selectedNode;
    }

    public ResponseNodesForBlock selectNodeRoundRobin(Set<String> existingNodeUrls, int n, String requestingNode) {
    	logger.debug("NodeManager::selectNodeRoundRobin called");
    	logger.debug("existingNodeUrls={}", existingNodeUrls);
    	logger.debug("requestingNode={}", requestingNode);
    	if (requestingNode==null || existingNodeUrls==null) {
    		ResponseNodesForBlock response = new ResponseNodesForBlock(ResponseNodesForBlock.Status.INPUT_PARAMETERS_IS_NULL, new ArrayList<>());
    		return response;
    	}
    	
        List<DfsNode> selectedNodes = new ArrayList<>();
        ResponseNodesForBlock response = new ResponseNodesForBlock(ResponseNodesForBlock.Status.SUCCESS, selectedNodes);
        
        logger.debug("replication factor={}", n);
        
        // If the number of existing nodes is already greater than or equal to n, return an empty list.
        if (existingNodeUrls.size() >= n) {
        	response.setStatus(ResponseNodesForBlock.Status.ALREADY_ENOUGH_COPIES);
        	logger.debug("existingNode Count>={}", n);
        	logger.debug("no new node selected for block save.");
            return response;
        }

        // If the requesting node is not in the existing nodes, add it to the selectedNodes.
        if (!existingNodeUrls.contains(requestingNode)) {
            DfsNode requestingDfsNode = registeredNodes.get(requestingNode);
            if (requestingDfsNode != null) {
                selectedNodes.add(requestingDfsNode);
            }
        }

        // Collect nodes from registeredNodes using round-robin, skipping those already in existingNodeUrls.
        List<DfsNode> registeredNodesList = new ArrayList<>(registeredNodes.values());
        int registeredNodesSize = registeredNodesList.size();

    	logger.debug("Looping the registered node to select node.");
    	logger.debug("registeredNodesSize={}", registeredNodesSize);
    	
        for (int i = 0; i < registeredNodesSize && selectedNodes.size() + existingNodeUrls.size() < n; i++) {
            DfsNode candidateNode = registeredNodesList.get(roundRobinIndex);
            
            logger.debug(" i={}, candidateNode is {}", i, candidateNode.getContainerUrl());
            logger.debug("selectedNodes.size()={}   existingNodeUrls.size()={}", selectedNodes.size(), existingNodeUrls.size());
            
            roundRobinIndex = (roundRobinIndex + 1) % registeredNodesSize;

            // Check if the candidate node is already in existingNodeUrls or selectedNodes.
            boolean isAlreadyExisting = existingNodeUrls.contains(candidateNode.getContainerUrl());
            boolean isAlreadySelected = selectedNodes.stream()
                    .anyMatch(node -> node.getContainerUrl().equals(candidateNode.getContainerUrl()));
            logger.debug("candidateNode isAlreadyExisting={}, isAlreadySelected={}", isAlreadyExisting, isAlreadySelected);
            
            // Add the candidate if it's not in existingNodeUrls or selectedNodes, and it's not the requesting node.
            if (!isAlreadyExisting && !isAlreadySelected && !candidateNode.getContainerUrl().equals(requestingNode)) {
                selectedNodes.add(candidateNode);
                logger.debug("Node selected: {}", candidateNode.getContainerUrl());
            }
        }
        logger.debug("By end of selectNodeRoundRobin, existingNodeUrls={}", existingNodeUrls);
        logger.debug("                               selectedNodes={}", selectedNodes);
        
        // If no nodes were selected, update the status to NO_NODES_AVAILABLE.
        if (selectedNodes.isEmpty()) {
        	logger.debug("No node is selected.");
            response.setStatus(ResponseNodesForBlock.Status.NO_NODES_AVAILABLE);
        }

        return response;
    }




    public List<DfsNode> getRegisteredNodes() {
        return new ArrayList<>(registeredNodes.values());
    }
    
    public void clearRegisteredNodes() {
        registeredNodes.clear();
        deadNodes.clear();
    }

    @Scheduled(fixedRateString = "${dfs.metanode.healthcheck.rate:60000}")
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
            }
        });

        logger.info("Health check completed at {}. Total nodes monitored: {}.", now, registeredNodes.size());
    }

    public String getLocalUrlForNode(String containerUrl) {
    	if (containerUrl==null) return null;
    	return registeredNodes.get(containerUrl).getLocalUrl();
    }
    private void handleDeadNode(DfsNode deadNode) {
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Map<String, DfsNode> getDeadNodes() {
        return deadNodes;
    }


}
