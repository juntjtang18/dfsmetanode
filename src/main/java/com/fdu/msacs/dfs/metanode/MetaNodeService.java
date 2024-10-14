package com.fdu.msacs.dfs.metanode;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.fdu.msacs.dfs.metanode.mdb.BlockNode;
import com.fdu.msacs.dfs.metanode.mdb.BlockNodeMappingRepo;
import com.fdu.msacs.dfs.metanode.mdb.FileNodeMapping;
import com.fdu.msacs.dfs.metanode.mdb.FileNodeMappingRepo;
import com.fdu.msacs.dfs.metanode.mdb.NodeFileMapping;
import com.fdu.msacs.dfs.metanode.mdb.NodeFileMappingRepo;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;
import com.fdu.msacs.dfs.metanode.meta.DfsNode.HealthStatus;

@Service
public class MetaNodeService {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeService.class);
    
    private FileNodeMappingRepo fileNodeMappingRepo;
    private NodeFileMappingRepo nodeFileMappingRepo;
    private ConcurrentHashMap<String, DfsNode> registeredNodes;
    private int currentNodeIndex = 0;
    private BlockNodeMappingRepo blockNodeMapping;
    
    @Value("${dfs.metanode.healthcheck.down_threshold:6}") int HEALTH_CHECK_THRESHOLD; // seconds
    @Value("${dfs.metanode.healthcheck.warning_threshold:3}") int WARNING_THRESHOLD; // seconds before downgrading to WARNING

    public MetaNodeService(BlockNodeMappingRepo blockNodeMapping,
					        FileNodeMappingRepo fileNodeMappingRepo,
					        NodeFileMappingRepo nodeFileMappingRepo) 
    {
		this.registeredNodes = new ConcurrentHashMap<>();
		this.blockNodeMapping = blockNodeMapping;
		this.fileNodeMappingRepo = fileNodeMappingRepo;
		this.nodeFileMappingRepo = nodeFileMappingRepo;
	}
    
    public String registerNode(DfsNode node) {
        String nodeUrl = node.getContainerUrl();
        DfsNode existingNode = registeredNodes.get(nodeUrl);

        if (existingNode == null) {
            registeredNodes.put(nodeUrl, node);            
            logger.info("A new node registered: {}", nodeUrl);
            return "Node registered: " + nodeUrl;
        } else {
            existingNode.setHealthStatus(HealthStatus.HEALTHY);
            existingNode.setLastTimeReport(new Date()); // Update last report time using Date
            logger.info("Node heartbeat listened from: {}", nodeUrl);
            return "Received Heartbeat from " + nodeUrl;
        }
    }

    
    public String registerFileLocation(String filename, String nodeUrl) {
        // Update or create the FileNode entry
        FileNodeMapping fileNodeMapping = fileNodeMappingRepo.findById(filename)
                .orElse(new FileNodeMapping(filename));
        fileNodeMapping.getNodeUrls().add(nodeUrl);
        fileNodeMappingRepo.save(fileNodeMapping);

        // Update or create the NodeFile entry
        NodeFileMapping nodeFileMapping = nodeFileMappingRepo.findById(nodeUrl)
                .orElse(new NodeFileMapping(nodeUrl));
        nodeFileMapping.getFilenames().add(filename);
        nodeFileMappingRepo.save(nodeFileMapping);

        logger.info("File {} registered to : {}", filename, nodeUrl);
        return "File location registered: " + filename + " on " + nodeUrl;
    }

    
    public String registerBlockLocation(String hash, String nodeUrl) {
		logger.debug("MetaService: registerBlockLocation: {}->{}", hash, nodeUrl);
		
    	BlockNode blockNode = blockNodeMapping.findByHash(hash);
    	if (blockNode == null) {
    		blockNode = new BlockNode();
    		blockNode.setHash(hash);
    		blockNode.getNodeUrls().add(nodeUrl);
    	}
    	blockNodeMapping.save(blockNode);
        logger.debug("Block {} registered to : {}", hash, nodeUrl);
        logger.debug("Current blockNodeMapping: {}", blockNodeMapping.findAll());
        return "Block location registered: " + hash + " on " + nodeUrl;
    }

    public List<String> getNodesForFile(String filename) {
        FileNodeMapping fileNodeMapping = fileNodeMappingRepo.findById(filename).orElse(new FileNodeMapping());
        List<String> nodeUrls = new ArrayList<>(fileNodeMapping.getNodeUrls());
        logger.info("Searching the node for file {}, return {}", filename, nodeUrls);
        return nodeUrls;
    }
    
    public List<DfsNode> getReplicationNodes(String filename, String requestingNodeUrl) {
        logger.info("/metadata/get-replication-nodes called for filename: {} and requestingNodeUrl: {}", filename, requestingNodeUrl);

        // Step 1: Create a list of available nodes and filter out the requesting node
        List<DfsNode> availableNodes = registeredNodes.values().stream()
            .filter(node -> !node.getContainerUrl().equals(requestingNodeUrl))
            .toList();

        // Step 2: If more than 2 nodes remain, limit the list to 2 nodes
        if (availableNodes.size() > 2) {
            availableNodes = availableNodes.subList(0, 2);
        }

        logger.info("Available nodes after filtering and limiting: {}", availableNodes);
        return availableNodes;
    }


    public List<DfsNode> getRegisteredNodes() {
        return new ArrayList<DfsNode>(registeredNodes.values());
    }

    public List<String> getNodeFiles(String nodeUrl) {
        NodeFileMapping nodeFileMapping = nodeFileMappingRepo.findById(nodeUrl).orElse(new NodeFileMapping());
        List<String> files = new ArrayList<>(nodeFileMapping.getFilenames());
        return files;
    }

    public void clearCache() {
        fileNodeMappingRepo.deleteAll();
        nodeFileMappingRepo.deleteAll();
        logger.info("Cache cleared.");
    }

    public void clearRegisteredNodes() {
        registeredNodes.clear();
        logger.info("Registered nodes cleared.");
    }

	public List<String> getFileNodeMapping(String filename) {
		FileNodeMapping fnmap = fileNodeMappingRepo.findByFilename(filename);
		if (fnmap==null) {
			return null;
		}
		return new ArrayList<String>(fnmap.getNodeUrls());
	}
	
    public DfsNode selectNodeForUpload() {
        if (registeredNodes.isEmpty()) {
            return null;
        }

        List<DfsNode> nodeList = new ArrayList<>(registeredNodes.values());        
        DfsNode selectedNode = nodeList.get(currentNodeIndex);

        // Update currentNodeIndex based on the total weight
        currentNodeIndex = (currentNodeIndex + 1) % nodeList.size();

        return selectedNode;
    }
    
    // Scheduled task to check health status periodically
    //@Scheduled(fixedRate = "${dfs.healthcheck.rate:30000}") // Every 30 seconds
    @Scheduled(fixedRateString = "${dfs.metanode.healthcheck.rate:3000}") // Use the injected property with a default
    public void checkNodeHealth() {
        Date now = new Date(); // Use Date instead of LocalDateTime
        logger.info("Starting health check for registered nodes at {}.", now);

        registeredNodes.forEach((containerUrl, node) -> {
            long secondsSinceLastReport = (now.getTime() - node.getLastTimeReport().getTime()) / 1000; // Calculate seconds since last report
            logger.info("Checking node({}) - Last report: {} seconds ago, expected threshold: {}", 
                        node.getContainerUrl(), secondsSinceLastReport, WARNING_THRESHOLD);

            if (secondsSinceLastReport > HEALTH_CHECK_THRESHOLD) {
                logger.info("The node({}) is down. Removing from registered nodes.", node.getContainerUrl());
                registeredNodes.remove(containerUrl);
                
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
}
