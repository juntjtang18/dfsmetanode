package com.fdu.msacs.dfs.metanode;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;

@RestController
public class MetaNodeController {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeController.class);
    @Autowired
    private MetaNodeService metaNodeService;
    @Autowired
    private NodeManager nodeManager; // Reference to the NodeManager for node management

    @PostMapping("/metadata/register-node")
    public ResponseEntity<String> registerNode(@RequestBody DfsNode dfsNode) {
        String response = nodeManager.registerNode(dfsNode); // Delegate registration to NodeManager
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
        List<DfsNode> replicationNodes = nodeManager.getReplicationNodes(request.getFilename(), request.getRequestingNodeUrl());
        return ResponseEntity.ok(replicationNodes);
    }

    @GetMapping("/metadata/get-registered-nodes")
    public ResponseEntity<List<DfsNode>> getRegisteredNodes() {
        List<DfsNode> registeredNodes = nodeManager.getRegisteredNodes(); // Delegate retrieval to NodeManager
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
        List<String> nodes = metaNodeService.getFileNodeMapping(filename);
        return ResponseEntity.ok(nodes);
    }

    // Test utility end point
    @PostMapping("/metadata/clear-cache")
    public ResponseEntity<String> clearCache() {
        metaNodeService.clearCache(); // Clear all file-node mappings
        return ResponseEntity.ok("Cache cleared");
    }

    @PostMapping("/metadata/clear-registered-nodes")
    public ResponseEntity<String> clearRegisteredNodes() {
        nodeManager.clearRegisteredNodes(); // Clear all registered nodes through NodeManager
        return ResponseEntity.ok("Registered nodes cleared.");
    }

    @PostMapping("/metadata/upload-url")
    public ResponseEntity<UploadResponse> getUploadUrl(@RequestBody RequestUpload requestUpload) {
        String filename = requestUpload.getFilename();
        boolean fileExists = metaNodeService.checkFileExists(filename);

        // Select a node using Weighted Round Robin
        DfsNode selectedNode = nodeManager.selectNodeForUpload();

        if (selectedNode == null) {
            logger.info("No node selected by nodeManager. Service temporarily unavailable.");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                                 .body(new UploadResponse(fileExists, null));
        }

        // Construct the URL for the upload endpoint of the selected node
        String uploadUrl = selectedNode.getLocalUrl() + "/dfs/upload";
        logger.info("Upload URL: {}", uploadUrl);
        return ResponseEntity.ok(new UploadResponse(fileExists, uploadUrl));
    }


    @GetMapping("/metadata/pingsvr")
    public String pingSvr() {
        return "Metadata Server is running...";
    }

    // Inner class to represent the request for upload
    public static class RequestUpload {
        private String uuid; // UUID of the request
        private String filename; // Filename to upload

        public RequestUpload() {
        }

        public RequestUpload(String uuid, String filename) {
            this.uuid = uuid;
            this.filename = filename;
        }

        public String getUuid() {
            return uuid;
        }

        public String getFilename() {
            return filename;
        }
    }

    // Inner class to represent the response for file check
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UploadResponse {
        private boolean exists; // Flag indicating whether the file exists
        private String nodeUrl; // URL of the selected node
        
        public UploadResponse() {
        	this.exists = false;
        	this.nodeUrl = "";
        }

        // Constructor with @JsonCreator for Jackson
        @JsonCreator
        public UploadResponse(@JsonProperty("exists") boolean exists, @JsonProperty("nodeUrl") String nodeUrl) {
            this.exists = exists;
            this.nodeUrl = nodeUrl;
        }

        public boolean isExists() {
            return exists;
        }

        public String getNodeUrl() {
            return nodeUrl;
        }
    }
}
