package com.fdu.msacs.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.fdu.msacs.dfs.metanode.mdb.BlockNode;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@RestController
public class BlockMetaController {
    private static final Logger logger = LoggerFactory.getLogger(BlockMetaController.class);
    
    @Autowired
    private BlockMetaService blockMetaService;
    
    @PostMapping("/metadata/register-block-location")
    public ResponseEntity<String> registerBlockLocation(@RequestBody RequestBlockNode request) {
        logger.info("/metadata/register-block-location requested with: {}->{}", request.hash, request.nodeUrl);
        String hash = request.hash;
        String nodeUrl = request.nodeUrl;
        String response = blockMetaService.registerBlockLocation(hash, nodeUrl);
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/metadata/nodes-for-block")
    public ResponseEntity<List<DfsNode>> nodesForBlock(@RequestBody RequestNodesForBlock request) {
    	logger.info("/metadata/nodes-for-block requested for hash: {}", request.hash);
    	List<DfsNode> nodes = blockMetaService.checkReplicationAndSelectNodes(request.hash, request.requestingNodeUrl);
    	return ResponseEntity.ok(nodes);
    	
    }
    
    @GetMapping("/metadata/block-nodes/{hash}")
    public ResponseEntity<List<String>> blockNodes(@PathVariable String hash) {
    	logger.info("/metadata/block-nodes/{} requested.", hash);
    	BlockNode blockNode = blockMetaService.getBlockNodeByHash(hash);
    	List<String> nodeUrls = new ArrayList<String>();
    	if (blockNode!=null) {
    		nodeUrls.addAll(blockNode.getNodeUrls());
    	}
    	return ResponseEntity.ok(nodeUrls);
    }
    
    @DeleteMapping("/metadata/unregister-block/{hash}")
    public ResponseEntity<String> unregisterBlock(@PathVariable String hash) {
        logger.info("/metadata/unregister-block requested for hash: {}", hash);
        String response = blockMetaService.unregisterBlock(hash);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/metadata/unregister-block-from-node")
    public ResponseEntity<String> unregisterBlockFromNode(@RequestBody RequestUnregisterBlock request) {
        logger.info("/metadata/unregister-block-from-node requested with: {} from {}", request.hash, request.nodeUrl);
        String response = blockMetaService.unregisterBlockFromNode(request.hash, request.nodeUrl);
        return ResponseEntity.ok(response);
    }
    
    @DeleteMapping("/metadata/clear-all-block-nodes-mapping")
    public ResponseEntity<String> clearAllBlocks() {
        logger.info("/metadata/clear-all-blocks requested.");
        String response = blockMetaService.clearAllBlockNodes();
        return ResponseEntity.ok(response);
    }
    
    // inner class for request
    public static class RequestBlockNode {
        private String hash;
        private String nodeUrl;

        // Getters and Setters
        public String getHash() 			{ return hash; }
        public void setHash(String hash) 	{ this.hash = hash;        }
        public String getNodeUrl() 			{ return nodeUrl;        }
        public void setNodeUrl(String nodeUrl) { this.nodeUrl = nodeUrl; }
    }
    
    public static class RequestUnregisterBlock {
        public String hash;
        public String nodeUrl;

        public String getHash() {            return hash;        }
        public void setHash(String hash) {            this.hash = hash;        }
        public String getNodeUrl() {            return nodeUrl;        }
        public void setNodeUrl(String nodeUrl) {            this.nodeUrl = nodeUrl;        }
    }
    
    public static class RequestNodesForBlock {
    	public String hash;
    	public String requestingNodeUrl;
    	
        public String getHash() {            return hash;        }
        public void setHash(String hash) {            this.hash = hash;        }
        public String getNodeUrl() {            return requestingNodeUrl;        }
        public void setNodeUrl(String nodeUrl) {            this.requestingNodeUrl = nodeUrl;        }
    	
    }
}
