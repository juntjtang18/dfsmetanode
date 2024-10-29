package com.infolink.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.infolink.dfs.metanode.BlockMetaController.ResponseNodesForBlock;
import com.infolink.dfs.metanode.mdb.BlockNode;
import com.infolink.dfs.metanode.mdb.BlockNodeMappingRepo;
import com.infolink.dfs.shared.DfsNode;

import jakarta.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class BlockMetaService {
    private static final Logger logger = LoggerFactory.getLogger(BlockMetaService.class);
    @Autowired
    private AppConfig config;
    @Autowired
    private BlockNodeMappingRepo blockNodeMappingRepo;
    @Autowired
    private NodeManager nodeManager;
    private int replicationFactor;
    
    
    @PostConstruct
    public void postConstruct() {
    	this.replicationFactor = config.getReplicationFactor();
    }
    
    public String registerBlockLocation(String hash, String nodeUrl) {
        logger.debug("MetaService: registerBlockLocation: {}->{}", hash, nodeUrl);

        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        if (blockNode == null) {
            blockNode = new BlockNode();
            blockNode.setHash(hash);
            blockNode.getNodeUrls().add(nodeUrl);
        } else {
            blockNode.getNodeUrls().add(nodeUrl);
        }
        
        blockNodeMappingRepo.save(blockNode);
        logger.debug("Block {} registered to : {}", hash, nodeUrl);
        logger.debug("Current blockNodeMapping for hash: {}: ", hash);
        BlockNode bnode = blockNodeMappingRepo.findByHash(hash);
        for(String nodeurl : bnode.getNodeUrls()) {
        	logger.debug("    {}", nodeurl);
        }
        return "Block location registered: " + hash + " on " + nodeUrl;
    }
    
    public BlockNode getBlockNodeByHash(String blockHash) {
        logger.debug("Fetching block node for hash: {}", blockHash);
        BlockNode blockNode = blockNodeMappingRepo.findByHash(blockHash);
        if (blockNode != null) {
            logger.debug("Block node found for hash: {} -> {}", blockHash, blockNode);
        } else {
            logger.debug("No block node found for hash: {}", blockHash);
        }
        return blockNode;
    }
    
    public Set<String> blockExists(String hash) {
        logger.debug("Checking if block exists for hash: {}", hash);
        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        if (blockNode != null) {
            logger.debug("Block exists for hash: {}, with node URLs: {}", hash, blockNode.getNodeUrls());
            return blockNode.getNodeUrls();
        } else {
            logger.debug("No block found for hash: {}", hash);
            return null;
        }
    }
    
    public ResponseNodesForBlock checkBlockReplicationAndSelectNodes(String hash, String requestingNodeUrl) {
        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        
        //logger.debug("checkReplicationAndSelectNodes requested. ");
        logger.debug(" blockNodeMapping for hash:({}) is {}", hash, blockNode);
        logger.debug("requestingNodeUrl is {}", requestingNodeUrl);
        //List<DfsNode> registeredNodes = nodeManager.getRegisteredNodes();
        //logger.debug("Registered nodes now: {} ", registeredNodes);
        
        Set<String> existingNodes = (blockNode != null) ? blockNode.getNodeUrls() : new HashSet<String>();
        if (existingNodes==null) existingNodes = new HashSet<>();
        ResponseNodesForBlock response = nodeManager.selectNodeRoundRobin(existingNodes, replicationFactor, requestingNodeUrl);
        return response;
    }


    public String unregisterBlock(String hash) {
        logger.debug("Unregistering block with hash: {}", hash);
        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        if (blockNode != null) {
            blockNodeMappingRepo.delete(blockNode);
            logger.debug("Block with hash: {} has been unregistered", hash);
            return "Block unregistered: " + hash;
        } else {
            logger.debug("No block found to unregister for hash: {}", hash);
            return "No block found for hash: " + hash;
        }
    }

    public String unregisterBlockFromNode(String hash, String nodeUrl) {
        logger.debug("Unregistering block with hash: {} from node URL: {}", hash, nodeUrl);
        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        
        if (blockNode == null) {
            logger.debug("No block found for hash: {}", hash);
            return "No block found for hash: " + hash;
        }

        if (!blockNode.getNodeUrls().remove(nodeUrl)) {
            logger.debug("Node URL: {} not found for block with hash: {}", nodeUrl, hash);
            return "Node URL: " + nodeUrl + " not found for block with hash: " + hash;
        }

        if (blockNode.getNodeUrls().isEmpty()) {
            blockNodeMappingRepo.delete(blockNode);
            logger.debug("No more nodes left for block with hash: {}. Block has been fully unregistered.", hash);
            return "Block fully unregistered as no node URLs remain: " + hash;
        }

        blockNodeMappingRepo.save(blockNode);
        logger.debug("Node URL: {} removed from block with hash: {}", nodeUrl, hash);
        return "Node URL removed from block: " + nodeUrl + " for hash: " + hash;
    }
    
    public String clearAllBlockNodes() {
        logger.debug("Clearing all block node mappings.");
        blockNodeMappingRepo.deleteAll();
        logger.debug("All block node mappings have been cleared.");
        return "All block nodes have been cleared.";
    }
}
