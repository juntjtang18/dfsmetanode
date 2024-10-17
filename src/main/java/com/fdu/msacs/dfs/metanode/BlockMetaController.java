package com.fdu.msacs.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

@RestController
public class BlockMetaController {
    private static final Logger logger = LoggerFactory.getLogger(BlockMetaController.class);
    
    @Autowired
    private BlockMetaService blockService;
    
    @PostMapping("/metadata/register-block-location")
    public ResponseEntity<String> registerBlockLocation(@RequestBody RequestBlockNode request) {
        logger.info("/metadata/register-block-location requested with: {}->{}", request.hash, request.nodeUrl);
        String hash = request.hash;
        String nodeUrl = request.nodeUrl;
        String response = blockService.registerBlockLocation(hash, nodeUrl);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/metadata/block-exists/{hash}")
    public ResponseEntity<Set<String>> blockExists(@PathVariable String hash) {
        logger.info("/metadata/block-exists requested for hash: {}", hash);
        Set<String> nodeUrls = blockService.blockExists(hash);
        // Return an empty set if no block exists for the given hash
        return ResponseEntity.ok(nodeUrls != null ? nodeUrls : Set.of());
    }

    @DeleteMapping("/metadata/unregister-block/{hash}")
    public ResponseEntity<String> unregisterBlock(@PathVariable String hash) {
        logger.info("/metadata/unregister-block requested for hash: {}", hash);
        String response = blockService.unregisterBlock(hash);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/metadata/unregister-block-from-node")
    public ResponseEntity<String> unregisterBlockFromNode(@RequestBody RequestUnregisterBlock request) {
        logger.info("/metadata/unregister-block-from-node requested with: {} from {}", request.hash, request.nodeUrl);
        String response = blockService.unregisterBlockFromNode(request.hash, request.nodeUrl);
        return ResponseEntity.ok(response);
    }
    
    
    // inner class for request
    public static class RequestBlockNode {
        private String hash;
        private String nodeUrl;

        // Getters and Setters
        public String getHash() {
            return hash;
        }

        public void setHash(String hash) {
            this.hash = hash;
        }

        public String getNodeUrl() {
            return nodeUrl;
        }

        public void setNodeUrl(String nodeUrl) {
            this.nodeUrl = nodeUrl;
        }
    }
    
    public static class RequestUnregisterBlock {
        public String hash;
        public String nodeUrl;

        // Getters and Setters
        public String getHash() {
            return hash;
        }

        public void setHash(String hash) {
            this.hash = hash;
        }

        public String getNodeUrl() {
            return nodeUrl;
        }

        public void setNodeUrl(String nodeUrl) {
            this.nodeUrl = nodeUrl;
        }
    }

}
