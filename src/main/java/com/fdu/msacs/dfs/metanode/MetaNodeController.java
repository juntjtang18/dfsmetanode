package com.fdu.msacs.dfs.metanode;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MetaNodeController {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeController.class);

    private final MetaNodeService metaNodeService;

    public MetaNodeController(MetaNodeService metaNodeService) {
        this.metaNodeService = metaNodeService;
    }
    
    @PostMapping("/metadata/register-node")
    public ResponseEntity<String> registerNode(@RequestBody DfsNode dfsNode) {
        String response = metaNodeService.registerNode(dfsNode);

        if (response.contains("already registered")) {
            return ResponseEntity.status(200).body(response); // Conflict status
        }
        return ResponseEntity.ok(response);
    }

    @PostMapping("/metadata/register-file-location")
    public ResponseEntity<String> registerFileLocation(@RequestBody RequestFileLocation request) {
        String filename = request.getFilename();
        String nodeUrl = request.getNodeUrl();
        String response = metaNodeService.registerFileLocation(filename, nodeUrl);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/metadata/get-replication-nodes")
    public ResponseEntity<List<DfsNode>> getReplicationNodes(@RequestBody RequestReplicationNodes request) {
        List<DfsNode> replicationNodes = metaNodeService.getReplicationNodes(request.getFilename(), request.getRequestingNodeUrl());
        return ResponseEntity.ok(replicationNodes);
    }

    @GetMapping("/metadata/get-registered-nodes")
    public ResponseEntity<List<DfsNode>> getRegisteredNodes() {
        List<DfsNode> registeredNodes = metaNodeService.getRegisteredNodes();
        return ResponseEntity.ok(registeredNodes);
    }
    
    @GetMapping("/metadata/nodes-for-file/{filename}")
    public ResponseEntity<List<String>> getNodesForFile(@PathVariable String filename) {
        List<String> nodeAddresses = metaNodeService.getNodesForFile(filename);
        return ResponseEntity.ok(nodeAddresses);
    }

    @PostMapping("/metadata/get-node-files")
    public ResponseEntity<List<String>> getNodeFiles(@RequestBody RequestNode request) {
        List<String> files = metaNodeService.getNodeFiles(request.getNodeUrl());
        return ResponseEntity.ok(files);
    }
    
    @GetMapping("/metadata/get-file-node-mapping/{filename}")
    public ResponseEntity<List<String>> getFileNodeMapping(@PathVariable String filename) {
        List<String> nodes = metaNodeService.getFileNodes(filename);
        return ResponseEntity.ok(nodes);
    }

    //Test utility end point
    @PostMapping("/metadata/clear-cache")
    public ResponseEntity<String> clearCache() {
        metaNodeService.clearCache(); // Clear all file-node mappings
        return ResponseEntity.ok("Cache cleared");
    }

    @PostMapping("/metadata/clear-registered-nodes")
    public ResponseEntity<String> clearRegisteredNodes() {
        metaNodeService.clearRegisteredNodes(); // Clear all registered nodes
        return ResponseEntity.ok("Registered nodes cleared.");
    }

    @GetMapping("/metadata/upload-url")
    public String getUploadUrl() {
        // Select a node using Weighted Round Robin
        DfsNode selectedNode = metaNodeService.selectNodeForUpload();
        if (selectedNode == null) {
            logger.error("No registered nodes available for upload.");
            return "No registered nodes available for upload.";
        }

        // Construct the URL for the upload endpoint of the selected node
        // if it's in development mode, use localhost, as the network is not working and using host port mapping to access 
        String uploadUrl = "http://localhost:" + selectedNode.getLocalUrl() + "/dfs/upload"; // Change '/upload' to your actual upload endpoint
        
        logger.info("Upload URL for node {}: {}", selectedNode, uploadUrl);
        return uploadUrl;
    }

    @GetMapping("/metadata/pingsvr")
    public String pingSvr() {
        return "Metadata Server is running...";
    }
}
