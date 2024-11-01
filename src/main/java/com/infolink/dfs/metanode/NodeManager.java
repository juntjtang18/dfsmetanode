package com.infolink.dfs.metanode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.infolink.dfs.metanode.BlockMetaController.ResponseNodesForBlock;
import com.infolink.dfs.shared.DfsNode;
import com.infolink.dfs.metanode.event.DeadNodeEvent;

@Service
public class NodeManager {
    private static final Logger logger = LoggerFactory.getLogger(NodeManager.class);
    
    private ConcurrentHashMap<String, DfsNode> registeredNodes;
    private ConcurrentHashMap<String, DfsNode> deadNodes;
    private int currentNodeIndex = 0;
    //private ExecutorService executorService;
    private int roundRobinIndex = 0;

    @Value("${dfs.node.heartbeat.rate:10000}") 
    private int HEALTH_CHECK_THRESHOLD;
    
    @Autowired
    private ApplicationEventPublisher eventPublisher;
    @Autowired
    public SimpMessagingTemplate messagingTemplate;
    //@Autowired
    public ObjectMapper objectMapper;  
    
    public NodeManager() {
        this.registeredNodes = new ConcurrentHashMap<>();
        this.deadNodes = new ConcurrentHashMap<>();
        this.objectMapper = new ObjectMapper();
    }

    public String registerNode(DfsNode node) {
        String nodeUrl = node.getContainerUrl();
        DfsNode existingNode = registeredNodes.get(nodeUrl);
        String returnMsg = "";
        
        registeredNodes.put(nodeUrl, node);
        boolean refreshNode = false;
        
        if (deadNodes.containsKey(nodeUrl)) {
            deadNodes.remove(nodeUrl);
            logger.debug("A dead node revives: {}", nodeUrl);
            returnMsg = "A dead node revives: " + nodeUrl;
            refreshNode = true;
        } else {
        	
	        if (existingNode == null) {
	            returnMsg = "Node registered: " + nodeUrl;
	            refreshNode = true;
	        } else {
	            returnMsg = "Received Heartbeat from " + nodeUrl;
	            if(existingNode.getBlockCount() != node.getBlockCount()) {
	            	refreshNode = true;
	            }
	        }
        }
        
        if (refreshNode) {
			invokeClient();
        }
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
    
    public ResponseNodesForBlock selectNodeBasedOnBlockCount(Set<String> existingNodeUrls, int n, String requestingNodeUrl) {
        logger.debug("NodeManager::selectNodeBasedOnBlockCount called");
        logger.debug("existingNodeUrls={}", existingNodeUrls);

        // Validate inputs early and return appropriate response if invalid
        if (existingNodeUrls == null) {
            return new ResponseNodesForBlock(ResponseNodesForBlock.Status.INPUT_PARAMETERS_IS_NULL, new ArrayList<>());
        }

        List<DfsNode> selectedNodes = new ArrayList<>();
        ResponseNodesForBlock response = new ResponseNodesForBlock(ResponseNodesForBlock.Status.SUCCESS, selectedNodes);
        
        logger.debug("Replication factor (n)={}", n);
        
        
        //TODO: first, remove the dead nodes from existingNodeUrls
        Set<String> activeNodeUrls = new HashSet<>(existingNodeUrls);
        activeNodeUrls.removeAll(deadNodes.keySet());        
        
        // Check if the existing nodes already meet or exceed the replication factor
        if (activeNodeUrls.size() >= n) {
            response.setStatus(ResponseNodesForBlock.Status.ALREADY_ENOUGH_COPIES);
            logger.debug("existingNode Count >= {}", n);
            logger.debug("No new node selected for block save.");
            return response;
        }

        // Calculate how many more nodes are needed to reach the desired count
        int nodesNeeded = n - activeNodeUrls.size();

        // Filter registered nodes not in activeNodeUrls, then sort by blockCount
        List<DfsNode> candidateNodes = registeredNodes.values().stream()
            .filter(node -> !activeNodeUrls.contains(node.getContainerUrl()))  // Exclude existing nodes
            .sorted(Comparator.comparingLong(DfsNode::getBlockCount))  // Sort by blockCount in ascending order
            .collect(Collectors.toList());

        logger.debug("Candidates available for selection (sorted by blockCount): {}", candidateNodes);

        // Select the required number of nodes from sorted candidates
        for (int i = 0; i < nodesNeeded && i < candidateNodes.size(); i++) {
            DfsNode selectedNode = candidateNodes.get(i);
            selectedNodes.add(selectedNode);
            logger.debug("Node selected based on lowest blockCount: {}", selectedNode.getContainerUrl());
        }

        // If no nodes were selected, update the status to NO_NODES_AVAILABLE
        if (selectedNodes.isEmpty()) {
            logger.debug("No nodes selected based on blockCount criteria.");
            response.setStatus(ResponseNodesForBlock.Status.NO_NODES_AVAILABLE);
        }

        logger.debug("Selected nodes to store block: {}", selectedNodes);
        return response;
    }

    public ResponseNodesForBlock selectNodesForBlockRoundRobin(Set<String> existingNodeUrls, int n, String requestingNode) {
    	logger.debug("NodeManager::selectNodesForBlockRoundRobin called");
    	logger.debug("existingNodeUrls={}", existingNodeUrls);
    	logger.debug("requestingNode={}", requestingNode);
    	
    	if (requestingNode==null || existingNodeUrls==null) {
    		ResponseNodesForBlock response = new ResponseNodesForBlock(ResponseNodesForBlock.Status.INPUT_PARAMETERS_IS_NULL, new ArrayList<>());
    		return response;
    	}
    	
        List<DfsNode> selectedNodes = new ArrayList<>();
        ResponseNodesForBlock response = new ResponseNodesForBlock(ResponseNodesForBlock.Status.SUCCESS, selectedNodes);
        
        logger.debug("replication factor={}", n);
        
        // Remove dead nodes from existingNodeUrls
        Set<String> activeNodeUrls = new HashSet<>(existingNodeUrls);
        activeNodeUrls.removeAll(deadNodes.keySet());

        // If the number of existing nodes is already greater than or equal to n, return an empty list.
        if (activeNodeUrls.size() >= n) {
        	response.setStatus(ResponseNodesForBlock.Status.ALREADY_ENOUGH_COPIES);
        	logger.debug("existingNode Count>={}", n);
        	logger.debug("no new node selected for block save.");
            return response;
        }
        
        // If the requesting node is not in the existing nodes, add it to the selectedNodes.
        if (!activeNodeUrls.contains(requestingNode)) {
            DfsNode requestingDfsNode = registeredNodes.get(requestingNode);
            if (requestingDfsNode != null) {
                selectedNodes.add(requestingDfsNode);
            }
        }

        // Collect nodes from registeredNodes using round-robin, skipping those already in activeNodeUrls.
        List<DfsNode> registeredNodesList = new ArrayList<>(registeredNodes.values());
        int registeredNodesSize = registeredNodesList.size();

    	logger.debug("Looping the registered node to select node.");
    	logger.debug("registeredNodesSize={}", registeredNodesSize);
    	
        for (int i = 0; i < registeredNodesSize && selectedNodes.size() + activeNodeUrls.size() < n; i++) {
            DfsNode candidateNode = registeredNodesList.get(roundRobinIndex);
            
            logger.debug(" i={}, candidateNode is {}", i, candidateNode.getContainerUrl());
            logger.debug("selectedNodes.size()={}   activeNodeUrls.size()={}", selectedNodes.size(), activeNodeUrls.size());
            
            roundRobinIndex = (roundRobinIndex + 1) % registeredNodesSize;

            // Check if the candidate node is already in activeNodeUrls or selectedNodes.
            boolean isAlreadyExisting = activeNodeUrls.contains(candidateNode.getContainerUrl());
            boolean isAlreadySelected = selectedNodes.stream()
                    .anyMatch(node -> node.getContainerUrl().equals(candidateNode.getContainerUrl()));
            logger.debug("candidateNode isAlreadyExisting={}, isAlreadySelected={}", isAlreadyExisting, isAlreadySelected);
            
            // Add the candidate if it's not in activeNodeUrls or selectedNodes, and it's not the requesting node.
            if (!isAlreadyExisting && !isAlreadySelected && !candidateNode.getContainerUrl().equals(requestingNode)) {
                selectedNodes.add(candidateNode);
                logger.debug("Node selected: {}", candidateNode.getContainerUrl());
            }
        }
        logger.debug("By end of selectNodesForBlockRoundRobin, activeNodeUrls={}", activeNodeUrls);
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
    
    void invokeClient() {
        try {
            // Convert registeredNodes to JSON
            List<DfsNode> nodeList = registeredNodes.values().stream().collect(Collectors.toList());
            String json = objectMapper.writeValueAsString(nodeList); // Serialize to JSON
            messagingTemplate.convertAndSend("/topic/refresh-node", json); // Notify all clients about the update
            logger.debug("sending message to websocket:/topic/refresh-node. Message is {}", json);
            
        } catch (Exception e) {
            logger.error("Error converting registeredNodes to JSON", e);
        }

    }
    
    @Scheduled(fixedRateString = "${dfs.node.heartbeat.rate:10000}") // Execute every 10 seconds
    public void checkNodeHealth() {
        Date now = new Date();
        logger.info("Starting health check for registered nodes at {}.", now);

        // Use an iterator to avoid ConcurrentModificationException
        boolean refreshNode = false;
        for (Map.Entry<String, DfsNode> entry : registeredNodes.entrySet()) {
            DfsNode node = entry.getValue();
            long milliSecondsSinceLastReport = (now.getTime() - node.getLastTimeReport().getTime());

            if (milliSecondsSinceLastReport > HEALTH_CHECK_THRESHOLD + 1) {
                logger.info("The node({}) is down. Moving to deadNodes from registered nodes.", node.getContainerUrl());
                deadNodes.put(node.getContainerUrl(), node);
                registeredNodes.remove(node.getContainerUrl());
                eventPublisher.publishEvent(new DeadNodeEvent(node));
                refreshNode = true; // Set the flag to true
            }
        }
        //if (refreshNode) {
    	try {
			invokeClient();
		} catch (Exception e) {
			e.printStackTrace();
		}
        //}
        logger.info("Health check completed at {}. Total nodes monitored: {}.", now, registeredNodes.size());
    }

    public String getLocalUrlForNode(String containerUrl) {
    	if (containerUrl==null) return null;
    	return registeredNodes.get(containerUrl).getLocalUrl();
    }
    
    public Map<String, DfsNode> getDeadNodes() {
        return deadNodes;
    }

	public boolean isDeadNode(DfsNode deadNode) {
		return deadNodes.get(deadNode.getContainerUrl()) != null;
	}


}
