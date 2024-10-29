package com.infolink.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.infolink.dfs.metanode.mdb.BlockNode;
import com.infolink.dfs.shared.DfsNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@RestController
public class BlockMetaController {
    private static final Logger logger = LoggerFactory.getLogger(BlockMetaController.class);
    
    @Autowired
    private BlockMetaService blockMetaService;
    
    @PostMapping("/metadata/block/register-block-location")
    public ResponseEntity<String> registerBlockLocation(@RequestBody RequestBlockNode request) {
    	if (request==null || request.getHash()==null || request.getNodeUrl() == null) {
    		logger.debug("Bad request parameter. Either request is null or hash/nodeUrl is null.");
    		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Bad request parameter. Either request is null or hash/nodeUrl is null.");
    	}
    	
        logger.info("/metadata/register-block-location requested with: {}->{}", request.hash, request.nodeUrl);
        String hash = request.hash;
        String nodeUrl = request.nodeUrl;
        String response = blockMetaService.registerBlockLocation(hash, nodeUrl);
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/metadata/block/nodes-for-block")
    public ResponseEntity<ResponseNodesForBlock> nodesForBlock(@RequestBody RequestNodesForBlock request) {
    	logger.info("/metadata/block/nodes-for-block requested for hash: {}", request.hash);
    	logger.info("request = {} {}", request.getHash(), request.getNodeUrl());
    	
    	ResponseNodesForBlock responseNodesForBlock = blockMetaService.checkBlockReplicationAndSelectNodes(request.hash, request.nodeUrl);
    	return ResponseEntity.ok(responseNodesForBlock);    	
    }
    
    @GetMapping("/metadata/block/block-nodes/{hash}")
    public ResponseEntity<List<String>> blockNodes(@PathVariable String hash) {
    	logger.info("/metadata/block-nodes/{} requested.", hash);
    	BlockNode blockNode = blockMetaService.getBlockNodeByHash(hash);
    	List<String> nodeUrls = new ArrayList<String>();
    	if (blockNode!=null) {
    		nodeUrls.addAll(blockNode.getNodeUrls());
    	}
    	return ResponseEntity.ok(nodeUrls);
    }
    
    @DeleteMapping("/metadata/block/unregister-block/{hash}")
    public ResponseEntity<String> unregisterBlock(@PathVariable String hash) {
        logger.info("/metadata/unregister-block requested for hash: {}", hash);
        String response = blockMetaService.unregisterBlock(hash);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/metadata/block/unregister-block-from-node")
    public ResponseEntity<String> unregisterBlockFromNode(@RequestBody RequestUnregisterBlock request) {
        logger.info("/metadata/unregister-block-from-node requested with: {} from {}", request.hash, request.nodeUrl);
        String response = blockMetaService.unregisterBlockFromNode(request.hash, request.nodeUrl);
        return ResponseEntity.ok(response);
    }
    
    @DeleteMapping("/metadata/block/clear-all-block-nodes-mapping")
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
    	public String nodeUrl;
    	
        public String getHash() {            return hash;        }
        public void setHash(String hash) {            this.hash = hash;        }
        public String getNodeUrl() {            return nodeUrl;        }
        public void setNodeUrl(String nodeUrl) {            this.nodeUrl = nodeUrl;        }
    	
    }
    
    public static class ResponseNodesForBlock {
        public static enum Status {
            SUCCESS,
            NO_NODES_AVAILABLE,
            ALREADY_ENOUGH_COPIES,
            INPUT_PARAMETERS_IS_NULL
        }

        private Status status;
        private List<DfsNode> nodes; // List of selected nodes
        
        public ResponseNodesForBlock() {};
        
        public ResponseNodesForBlock(Status status, List<DfsNode> nodes) {
        	this.status = status;
        	this.nodes = nodes;
        }
		public List<DfsNode> getNodes() 		{		return nodes;		}
		public void setNodes(List<DfsNode> nodes) {			this.nodes = nodes;		}
		public Status getStatus() 				{			return status;		}
		public void setStatus(Status status) 	{			this.status = status;		}
    }
}
