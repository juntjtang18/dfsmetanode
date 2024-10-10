package com.fdu.msacs.dfsmetanode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriUtils;

@RestController
@RequestMapping("/metadata")
public class MetadataController {
    private static final Logger logger = LoggerFactory.getLogger(MetadataController.class);

    private Map<String, List<String>> fileNodeMap = new HashMap<>();
    private Map<String, List<String>> nodeFileMap = new HashMap<>();
    private List<String> registeredNodes = new ArrayList<>();

    @PostMapping("/register-node")
    public ResponseEntity<String> registerNode(@RequestBody String nodeAddress) {
        registeredNodes.add(nodeAddress);
        logger.info("A new nodes registered: {} ", nodeAddress);
        
        return ResponseEntity.ok("Node registered: " + nodeAddress);
    }

    @PostMapping("/register-file-location")
    public ResponseEntity<String> registerFileLocation(@RequestBody RequestFileLocation request) {
        String filename = request.getFilename();
        String nodeUrl = request.getNodeUrl();

        // Add node URL to the list of nodes storing this file
        fileNodeMap.computeIfAbsent(filename, k -> new ArrayList<>()).add(nodeUrl);
        nodeFileMap.computeIfAbsent(nodeUrl, k-> new ArrayList<>()).add(filename);
        logger.info("File {} registered to : {}", filename, nodeUrl);
        
        return ResponseEntity.ok("File location registered: " + filename + " on " + nodeUrl);
    }

    @GetMapping("/nodes-for-file/{filename}")
    public ResponseEntity<List<String>> getNodesForFile(@PathVariable String filename) {
        List<String> nodeAddresses = fileNodeMap.get(filename);
        logger.info("search the node for file {}, return {}", filename, nodeAddresses);
        return ResponseEntity.ok(nodeAddresses);
    }

    @PostMapping("/get-replication-nodes")
    public ResponseEntity<List<String>> getReplicationNodes(@RequestBody RequestReplicationNodes request) {
        String filename = request.getFilename();
        String requestingNodeUrl = request.getRequestingNodeUrl();

        logger.info("/metadata/get-replication-nodes called for filename: {} and requestingNodeUrl: {}", filename, requestingNodeUrl);
        
        // Decode the requestingNodeUrl to ensure it matches the format in registeredNodes
        String decodedRequestingNodeUrl = UriUtils.decode(requestingNodeUrl, StandardCharsets.UTF_8);
        
        // Get a list of all registered nodes and remove the requesting node
        List<String> availableNodes = new ArrayList<>(registeredNodes);
        
        logger.info("Available nodes before excluding the requesting node: {}", availableNodes);
        
        // Remove the requesting node
        availableNodes.remove(decodedRequestingNodeUrl);

        logger.info("Available nodes after excluding the requesting node: {}", availableNodes);
        
        // Exclude nodes that already have the file
        List<String> nodesWithFile = fileNodeMap.getOrDefault(filename, new ArrayList<>());
        availableNodes.removeAll(nodesWithFile);

        logger.info("Available nodes after excluding those with the file: {}", availableNodes);

        return ResponseEntity.ok(availableNodes);
    }

    
    @GetMapping("/get-registered-nodes")
    public ResponseEntity<List<String>> getRegisteredNodes() {
    	return ResponseEntity.ok(registeredNodes);
    }
    
    @PostMapping("/get-node-files")
    public ResponseEntity<List<String>> getNodeFiles(@RequestBody RequestNode request) {
    	logger.debug("Post /metadata/get-node-fiels/ {} ...", request);
    	List<String> nodeFiles = nodeFileMap.getOrDefault(request.getNodeUrl(), new ArrayList<String>());
    	return ResponseEntity.ok(nodeFiles);
    }
    
    @GetMapping("/get-file-node-mapping/{filename}")
    public ResponseEntity<List<String>> getFileNodeMapping(@PathVariable String filename) {
    	logger.info("/metadata/get-file-node-mapping/{} called...", filename);
    	List<String> nodeList = fileNodeMap.getOrDefault(filename, new ArrayList<String>());
    	logger.info("nodes {} has the file {}", nodeList, filename);
    	return ResponseEntity.ok(nodeList);
    }
    @PostMapping("/clear-cache")
    public ResponseEntity<String> clearCache() {
        fileNodeMap.clear();
        nodeFileMap.clear();
        logger.info("Cache cleared.");
        
        return ResponseEntity.ok("Cache cleared");
    }
    
    @PostMapping("/clear-registered-nodes")
    public ResponseEntity<String> clearRegisteredNodes() {
    	registeredNodes.clear();
    	logger.info("Registered nodes cleared.");
    	return ResponseEntity.ok("Registered nodes cleared.");
    }
    
    @GetMapping("/pingsvr")
    public String pingSvr() {
    	return "Metadata Server is running...";
    }
}
