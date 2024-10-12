package com.fdu.msacs.dfsmetanode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet; // Import HashSet
import java.util.List;
import java.util.Map;
import java.util.Set; // Import Set

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
public class MetadataController {
    private static final Logger logger = LoggerFactory.getLogger(MetadataController.class);

    private Map<String, List<String>> fileNodeMap = new HashMap<>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private Map<String, List<String>> nodeFileMap = new HashMap();
    private Set<String> registeredNodes = new HashSet<>(); // Change List to Set

    @PostMapping("/metadata/register-node")
    public ResponseEntity<String> registerNode(@RequestBody RequestNode request) {
        String nodeAddress = request.getNodeUrl();

        // Attempt to register the node and check if it was already registered
        if (registeredNodes.add(nodeAddress)) { // Returns true if added, false if already present
            logger.info("A new node registered: {}", nodeAddress);
            return ResponseEntity.ok("Node registered: " + nodeAddress);
        } else {
            logger.warn("Node already registered: {}", nodeAddress);
            return ResponseEntity.status(409).body("Node already registered: " + nodeAddress); // Conflict status
        }
    }

    @PostMapping("/metadata/register-file-location")
    public ResponseEntity<String> registerFileLocation(@RequestBody RequestFileLocation request) {
        String filename = request.getFilename();
        String nodeUrl = request.getNodeUrl();

        // Add node URL to the list of nodes storing this file
        fileNodeMap.computeIfAbsent(filename, k -> new ArrayList<>()).add(nodeUrl);
        nodeFileMap.computeIfAbsent(nodeUrl, k -> new ArrayList<>()).add(filename);
        logger.info("File {} registered to : {}", filename, nodeUrl);
        // Log the contents of fileNodeMap and nodeFileMap
        logger.info("Current fileNodeMap: {}", fileNodeMap);
        logger.info("Current nodeFileMap: {}", nodeFileMap);

        return ResponseEntity.ok("File location registered: " + filename + " on " + nodeUrl);
    }

    @GetMapping("/metadata/nodes-for-file/{filename}")
    public ResponseEntity<List<String>> getNodesForFile(@PathVariable String filename) {
        List<String> nodeAddresses = fileNodeMap.get(filename);
        logger.info("Searching the node for file {}, return {}", filename, nodeAddresses);
        return ResponseEntity.ok(nodeAddresses);
    }

    @PostMapping("/metadata/get-replication-nodes")
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

    @GetMapping("/metadata/get-registered-nodes")
    public ResponseEntity<Set<String>> getRegisteredNodes() { // Change return type to Set
        return ResponseEntity.ok(registeredNodes);
    }

    @PostMapping("/metadata/get-node-files")
    public ResponseEntity<List<String>> getNodeFiles(@RequestBody RequestNode request) {
        logger.debug("Post /metadata/get-node-files/ {} ...", request);
        List<String> nodeFiles = nodeFileMap.getOrDefault(request.getNodeUrl(), new ArrayList<String>());
        return ResponseEntity.ok(nodeFiles);
    }

    @GetMapping("/metadata/get-file-node-mapping/{filename}")
    public ResponseEntity<List<String>> getFileNodeMapping(@PathVariable String filename) {
        logger.info("/metadata/get-file-node-mapping/{} called...", filename);
        List<String> nodeList = fileNodeMap.getOrDefault(filename, new ArrayList<String>());
        logger.info("nodes {} has the file {}", nodeList, filename);
        return ResponseEntity.ok(nodeList);
    }

    @PostMapping("/metadata/clear-cache")
    public ResponseEntity<String> clearCache() {
        fileNodeMap.clear();
        nodeFileMap.clear();
        logger.info("Cache cleared.");

        return ResponseEntity.ok("Cache cleared");
    }

    @PostMapping("/metadata/clear-registered-nodes")
    public ResponseEntity<String> clearRegisteredNodes() {
        registeredNodes.clear();
        logger.info("Registered nodes cleared.");
        return ResponseEntity.ok("Registered nodes cleared.");
    }

    @GetMapping("/metadata/pingsvr")
    public String pingSvr() {
        return "Metadata Server is running...";
    }
}
