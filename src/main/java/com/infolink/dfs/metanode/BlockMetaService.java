package com.infolink.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;

import com.infolink.dfs.metanode.BlockMetaController.ResponseNodesForBlock;
import com.infolink.dfs.metanode.mdb.BlockNode;
import com.infolink.dfs.shared.DfsNode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class BlockMetaService {
    private static final Logger logger = LoggerFactory.getLogger(BlockMetaService.class);
    
    @Autowired
    private AppConfig config;
    
    @Autowired
    private RedisTemplate<String, BlockNode> redisTemplate;
    
    @Autowired
    private NodeManager nodeManager;
    
    private int replicationFactor;

    private static final String BLOCK_NODE_PREFIX = "BlockNode:";

    @PostConstruct
    public void postConstruct() {
        this.replicationFactor = config.getReplicationFactor();
    }

    public String registerBlockLocation(String hash, String nodeUrl) {
        logger.debug("MetaService: registerBlockLocation: {}->{}", hash, nodeUrl);

        String redisKey = BLOCK_NODE_PREFIX + hash;
        BlockNode blockNode = redisTemplate.opsForValue().get(redisKey);

        if (blockNode == null) {
            blockNode = new BlockNode();
            blockNode.setHash(hash);
        }
        blockNode.getNodeUrls().add(nodeUrl);
        redisTemplate.opsForValue().set(redisKey, blockNode);

        logger.debug("Block {} registered to : {}", hash, nodeUrl);
        logger.debug("Current blockNodeMapping for hash: {}: {}", hash, blockNode.getNodeUrls());
        return "Block location registered: " + hash + " on " + nodeUrl;
    }

    public BlockNode getBlockNodeByHash(String blockHash) {
        logger.debug("Fetching block node for hash: {}", blockHash);
        BlockNode blockNode = redisTemplate.opsForValue().get(BLOCK_NODE_PREFIX + blockHash);

        if (blockNode != null) {
            logger.debug("Block node found for hash: {} -> {}", blockHash, blockNode);
        } else {
            logger.debug("No block node found for hash: {}", blockHash);
        }
        return blockNode;
    }

    public Set<String> blockExists(String hash) {
        logger.debug("Checking if block exists for hash: {}", hash);
        BlockNode blockNode = redisTemplate.opsForValue().get(BLOCK_NODE_PREFIX + hash);

        if (blockNode != null) {
            logger.debug("Block exists for hash: {}, with node URLs: {}", hash, blockNode.getNodeUrls());
            return blockNode.getNodeUrls();
        } else {
            logger.debug("No block found for hash: {}", hash);
            return null;
        }
    }

    public ResponseNodesForBlock checkBlockReplicationAndSelectNodes(String hash, String requestingNodeUrl) {
        BlockNode blockNode = redisTemplate.opsForValue().get(BLOCK_NODE_PREFIX + hash);
        // here there is a logic trap. the existingNodes could be dead ones, so the best way to do is filter out those dead nodes.
        
        Set<String> existingNodes = (blockNode != null) ? blockNode.getNodeUrls() : new HashSet<>();
        ResponseNodesForBlock response = nodeManager.selectNodeBasedOnBlockCount(existingNodes, replicationFactor, requestingNodeUrl);
        
        if (response != null && response.getNodes() != null) {
            logger.debug("The selected nodes are:");
            response.getNodes().forEach(node -> logger.debug("Selected node container URL: {}", node.getContainerUrl()));
        } else {
            logger.debug("No nodes were selected.");
        }
        
        return response;
    }

    public String unregisterBlock(String hash) {
        logger.debug("Unregistering block with hash: {}", hash);
        String redisKey = BLOCK_NODE_PREFIX + hash;

        if (redisTemplate.delete(redisKey)) {
            logger.debug("Block with hash: {} has been unregistered", hash);
            return "Block unregistered: " + hash;
        } else {
            logger.debug("No block found to unregister for hash: {}", hash);
            return "No block found for hash: " + hash;
        }
    }

    public String unregisterBlockFromNode(String hash, String nodeUrl) {
        logger.debug("Unregistering block with hash: {} from node URL: {}", hash, nodeUrl);
        String redisKey = BLOCK_NODE_PREFIX + hash;
        BlockNode blockNode = redisTemplate.opsForValue().get(redisKey);

        if (blockNode == null || !blockNode.getNodeUrls().remove(nodeUrl)) {
            logger.debug("Node URL: {} not found for block with hash: {}", nodeUrl, hash);
            return "Node URL: " + nodeUrl + " not found for block with hash: " + hash;
        }

        if (blockNode.getNodeUrls().isEmpty()) {
            redisTemplate.delete(redisKey);
            logger.debug("No more nodes left for block with hash: {}. Block has been fully unregistered.", hash);
            return "Block fully unregistered as no node URLs remain: " + hash;
        }

        redisTemplate.opsForValue().set(redisKey, blockNode);
        logger.debug("Node URL: {} removed from block with hash: {}", nodeUrl, hash);
        return "Node URL removed from block: " + nodeUrl + " for hash: " + hash;
    }

    public String clearAllBlockNodes() {
        logger.debug("Clearing all block node mappings.");
        Set<String> keys = redisTemplate.keys(BLOCK_NODE_PREFIX + "*");
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
        logger.debug("All block node mappings have been cleared.");
        return "All block nodes have been cleared.";
    }
}
