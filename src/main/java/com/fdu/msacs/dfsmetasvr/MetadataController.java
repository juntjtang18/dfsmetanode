package com.fdu.msacs.dfsmetasvr;

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

@RestController
@RequestMapping("/metadata")
public class MetadataController {
    private static final Logger logger = LoggerFactory.getLogger(MetadataController.class);

    private final Map<String, List<String>> fileNodeMap = new HashMap<>();
    private final List<String> nodes = new ArrayList<>();

    @PostMapping("/register-node")
    public ResponseEntity<String> registerNode(@RequestBody String nodeAddress) {
        nodes.add(nodeAddress);
        logger.info("A new nodes registered: {} ", nodeAddress);
        
        return ResponseEntity.ok("Node registered: " + nodeAddress);
    }

    @PostMapping("/register-file-location")
    public ResponseEntity<String> registerFileLocation(@RequestBody RequestFileLocation request) {
        String filename = request.getFilename();
        String nodeUrl = request.getNodeUrl();

        // Add node URL to the list of nodes storing this file
        fileNodeMap.computeIfAbsent(filename, k -> new ArrayList<>()).add(nodeUrl);
        logger.info("File {} registered to : {}", filename, nodeUrl);
        
        return ResponseEntity.ok("File location registered: " + filename + " on " + nodeUrl);
    }

    @GetMapping("/nodes-for-file/{filename}")
    public ResponseEntity<List<String>> getNodesForFile(@PathVariable String filename) {
        List<String> nodeAddresses = fileNodeMap.get(filename);
        logger.info("search the node for file {}, return {}", filename, nodeAddresses);
        return ResponseEntity.ok(nodeAddresses);
    }

    // New method to clear the cache
    @PostMapping("/clear-cache")
    public ResponseEntity<String> clearCache() {
        fileNodeMap.clear();
        logger.info("Cache cleared.");
        
        return ResponseEntity.ok("Cache cleared");
    }
    
    @GetMapping("/pingsvr")
    public String pingSvr() {
    	return "Metadata Server is running...";
    }
}
