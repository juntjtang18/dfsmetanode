package com.infolink.dfs.metanode;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.infolink.dfs.shared.DfsNode;

@RestController
public class NodeManageController {
    private static final Logger logger = LoggerFactory.getLogger(NodeManageController.class);
    @Autowired
    private NodeManager nodeManager; // Reference to the NodeManager for node management
    @Autowired
    private FileTreeManager fileTreeManager;
    
    @PostMapping("/metadata/register-node")
    public ResponseEntity<String> registerNode(@RequestBody DfsNode dfsNode) {
        String response = nodeManager.registerNode(dfsNode); // Delegate registration to NodeManager
        return ResponseEntity.ok(response);
    }

    @GetMapping("/metadata/get-registered-nodes")
    public ResponseEntity<List<DfsNode>> getRegisteredNodes() {
        List<DfsNode> registeredNodes = nodeManager.getRegisteredNodes(); // Delegate retrieval to NodeManager
        return ResponseEntity.ok(registeredNodes);
    }
    
    @GetMapping("/metadata/get-dead-nodes") 
    public ResponseEntity<List<DfsNode>> getDeadNodes() {
    	List<DfsNode> deadNodes = nodeManager.getDeadNodes();
    	return ResponseEntity.ok(deadNodes);
    }
    
    @PostMapping("/metadata/get-localurl-for-node")
    public ResponseEntity<String> getLocalUrlForNode(@RequestBody String containerUrl) {
    	if (containerUrl==null) {
    		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("The containerUrl is null.");
    	}
    	String localUrl = nodeManager.getLocalUrlForNode(containerUrl);
    	return ResponseEntity.status(HttpStatus.OK).body(localUrl);
    }
    
    @PostMapping("/metadata/clear-registered-nodes")
    public ResponseEntity<String> clearRegisteredNodes() {
        nodeManager.clearRegisteredNodes(); // Clear all registered nodes through NodeManager
        return ResponseEntity.ok("Registered nodes cleared.");
    }

	 @PostMapping("/metadata/upload-url")
	 public ResponseEntity<UploadResponse> getUploadUrl(@RequestBody RequestUpload requestUpload) {
	     String filename = requestUpload.getFilename();
	     String targetDir = requestUpload.getTargetDir();
	     
	     // Normalize the filename to a Linux-style path if it contains a Windows path format
	     filename = normalizePathToLinux(filename);
	     
	     String fullPath = targetDir + "/" + filename;
	     boolean fileExists = fileTreeManager.checkFileExistsByPath(fullPath);
	
	     if (fileExists) {
	         logger.info("File {} exists.", fullPath);
	     }
	
	     // Select a node using Weighted Round Robin
	     DfsNode selectedNode = nodeManager.selectNodeRoundRobin();
	
	     if (selectedNode == null) {
	         logger.info("No node selected by nodeManager. Service temporarily unavailable.");
	         return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
	                              .body(new UploadResponse(fileExists, null));
	     }
	
	     // Construct the URL for the upload endpoint of the selected node
	     String uploadUrl = selectedNode.getLocalUrl() + "/dfs/file/upload";
	     logger.info("Upload URL: {}", uploadUrl);
	     return ResponseEntity.ok(new UploadResponse(fileExists, uploadUrl));
	 }
	
	 private String normalizePathToLinux(String path) {
	     if (path.matches("^[a-zA-Z]:\\\\.*")) { // Detect Windows path with drive letter
	         path = path.replace("\\", "/"); // Replace backslashes with forward slashes
	         path = "/" + path.substring(0, 1).toLowerCase() + path.substring(2); // Change 'C:\' to '/c/'
	     }
	     return path;
	 }
	
	 /*
    @PostMapping("/metadata/dedupe-upload-url")
    public ResponseEntity<UploadResponse> getDedupeUploadUrl(@RequestBody RequestUpload requestUpload) {
        String filename = requestUpload.getFilename();
        String targetDir = requestUpload.getTargetDir();
        String fullPath = targetDir + "/" + filename;
        boolean fileExists = fileTreeManager.checkFileExistsByPath(fullPath);

        if (fileExists) {
        	logger.info("File {} exists.", fullPath);
        }

        DfsNode selectedNode = nodeManager.selectNodeRoundRobin();

        if (selectedNode == null) {
            logger.info("No node selected by nodeManager. Service temporarily unavailable.");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                                 .body(new UploadResponse(fileExists, null));
        }

        // Construct the URL for the upload endpoint of the selected node
        String uploadUrl = selectedNode.getLocalUrl() + "/dfs/file/upload";
        logger.info("Upload URL: {}", uploadUrl);
        return ResponseEntity.ok(new UploadResponse(fileExists, uploadUrl));
    }
    */
    @PostMapping("/metadata/download-url")
    public ResponseEntity<String> getDownloadUrl(@RequestBody String hash) {
        logger.info("Received request to get download URL for file with hash: {}", hash);
        
        boolean fileExists = fileTreeManager.checkFileExistsByHash(hash);
        if (!fileExists) {
            logger.warn("File with hash {} does not exist.", hash);
            ResponseEntity<String> response = ResponseEntity.status(HttpStatus.NO_CONTENT).body("File {" + hash + "} does not exist.");
            
            logger.info("Response for non-existent file: {}", response);
            return response;
        }

        logger.info("File with hash {} exists. Proceeding to select a node.", hash);
        DfsNode selectedNode = nodeManager.selectNodeRoundRobin();
        if (selectedNode == null) {
            logger.warn("No node selected by nodeManager for file with hash: {}. Service temporarily unavailable.", hash);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("No node available to download.");
        }

        String downloadUrl = selectedNode.getLocalUrl() + "/dfs/file/download";
        logger.info("Node selected: {}. Generated download URL: {}", selectedNode.getLocalUrl(), downloadUrl);

        return ResponseEntity.ok(downloadUrl);
    }


    @GetMapping("/metadata/pingsvr")
    public String pingSvr() {
        return "Metadata Server is running...";
    }

    // Inner class to represent the request for upload
    public static class RequestUpload {
        private String uuid; // UUID of the request
        private String filename; // Filename to upload
        private String targetDir;
        private String owner;
        
        public RequestUpload() {
        }

        public RequestUpload(String uuid, String filename, String targetDir, String owner) {
            this.uuid = uuid;
            this.filename = filename;
            this.setTargetDir(targetDir);
            this.setOwner(owner);
        }

        public String getUuid() 					{            return uuid;        }
        public String getFilename() 				{            return filename;        }
		public String getTargetDir() 				{			return targetDir;		}
		public void setTargetDir(String targetDir) 	{			this.targetDir = targetDir;		}
		public String getOwner() 					{			return owner;		}
		public void setOwner(String owner) 			{			this.owner = owner;		}
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
