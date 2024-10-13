package com.fdu.msacs.dfs.metanode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriUtils;

import com.fdu.msacs.dfs.metanode.mongodb.FileNodeMappingRepository;
import com.fdu.msacs.dfs.metanode.mongodb.FileNodes;
import com.fdu.msacs.dfs.metanode.mongodb.NodeFileMappingRepository;
import com.fdu.msacs.dfs.metanode.mongodb.NodeFiles;
import com.fdu.msacs.dfs.metanode.mongodb.RegisteredNode;
import com.fdu.msacs.dfs.metanode.mongodb.RegisteredNodeRepository;

@Service
public class MetaNodeService {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeService.class);
    
    @Autowired
    private FileNodeMappingRepository fileNodeMapping;
    @Autowired
    private NodeFileMappingRepository nodeFileMapping;
    @Autowired
    private RegisteredNodeRepository registeredNodeRepository;
    
    private Set<String> registeredNodes = new HashSet<>();
    private Map<String, Integer> nodeWeights = new HashMap<>();
    private int totalWeight = 0;
    private int currentNodeIndex = 0;

        public String registerNode(String nodeAddress) {
        if (registeredNodeRepository.findByNodeUrl(nodeAddress) == null) {
            RegisteredNode registeredNode = new RegisteredNode();
            registeredNode.setNodeUrl(nodeAddress);
            registeredNodeRepository.save(registeredNode);
            registeredNodes.add(nodeAddress);
            logger.info("A new node registered: {}", nodeAddress);
            return "Node registered: " + nodeAddress;
        } else {
            logger.warn("Node already registered: {}", nodeAddress);
            return "Node already registered: " + nodeAddress; // Conflict status
        }
    }

    public String registerFileLocation(String filename, String nodeUrl) {
        // Update or create the FileNode entry
        FileNodes fileNode = fileNodeMapping.findByFilename(filename);
        if (fileNode == null) {
            fileNode = new FileNodes();
            fileNode.setFilename(filename);
            fileNode.setNodeUrls(new ArrayList<>());
        }
        fileNode.getNodeUrls().add(nodeUrl);
        fileNodeMapping.save(fileNode);

        // Update or create the NodeFileMapping entry
        NodeFiles nodeFiles = nodeFileMapping.findByNodeUrl(nodeUrl);
        if (nodeFiles == null) {
            nodeFiles = new NodeFiles();
            nodeFiles.setNodeUrl(nodeUrl);
            nodeFiles.setFilenames(new ArrayList<>());
        }
        nodeFiles.getFilenames().add(filename);
        nodeFileMapping.save(nodeFiles);
        
        logger.info("File {} registered to : {}", filename, nodeUrl);
        return "File location registered: " + filename + " on " + nodeUrl;
    }

    public List<String> getNodesForFile(String filename) {
        FileNodes fileNode = fileNodeMapping.findByFilename(filename);
        List<String> nodeAddresses = fileNode != null ? fileNode.getNodeUrls() : new ArrayList<>();
        logger.info("Searching the node for file {}, return {}", filename, nodeAddresses);
        return nodeAddresses;
    }

    
    public List<String> getReplicationNodes(String filename, String requestingNodeUrl) {
        logger.info("/metadata/get-replication-nodes called for filename: {} and requestingNodeUrl: {}", filename, requestingNodeUrl);
        String decodedRequestingNodeUrl = UriUtils.decode(requestingNodeUrl, StandardCharsets.UTF_8);

        List<String> availableNodes = new ArrayList<>(registeredNodes);
        availableNodes.remove(decodedRequestingNodeUrl);

        FileNodes fileNode = fileNodeMapping.findByFilename(filename);
        if (fileNode != null) {
            availableNodes.removeAll(fileNode.getNodeUrls());
        }

        logger.info("Available nodes after filtering: {}", availableNodes);
        return availableNodes;
    }

    public Set<String> getRegisteredNodes() {
        registeredNodes.clear();
        registeredNodes.addAll(registeredNodeRepository.findAll().stream()
                .map(RegisteredNode::getNodeUrl)
                .toList());
        return registeredNodes;
    }

    public List<String> getNodeFiles(String nodeUrl) {
        List<String> files = new ArrayList<>();
        NodeFiles nodeFiles = nodeFileMapping.findByNodeUrl(nodeUrl);
        if (nodeFiles != null) {
            files.addAll(nodeFiles.getFilenames());
        }
        return files;
    }

    public void clearCache() {
        fileNodeMapping.deleteAll(); // Clear all file-node mappings
        nodeFileMapping.deleteAll();
        logger.info("Cache cleared.");
    }

    public void clearRegisteredNodes() {
        registeredNodeRepository.deleteAll(); // Clear all registered nodes
        registeredNodes.clear();
        logger.info("Registered nodes cleared.");
    }

	public List<String> getFileNodes(String filename) {
		List<String> nodes = new ArrayList<>();
		FileNodes fileNodes = fileNodeMapping.findByFilename(filename);
		if (fileNodes!=null) {
			nodes.addAll(fileNodes.getNodeUrls());
		}
		return nodes;
	}
	
    public String selectNodeForUpload() {
        // Refresh registered nodes
        getRegisteredNodes();

        // If no registered nodes, return null
        if (registeredNodes.isEmpty()) {
            return null;
        }

        List<String> nodeList = new ArrayList<>(registeredNodes);
        //int weight = nodeWeights.getOrDefault(nodeList.get(currentNodeIndex), 1);
        String selectedNode = nodeList.get(currentNodeIndex);

        // Update currentNodeIndex based on the total weight
        currentNodeIndex = (currentNodeIndex + 1) % nodeList.size();

        return selectedNode;
    }

    public void updateNodeWeights(Map<String, Integer> weights) {
        this.nodeWeights = weights;
        totalWeight = weights.values().stream().mapToInt(Integer::intValue).sum();
        logger.info("Node weights updated: {}", nodeWeights);
    }
}
