package com.infolink.dfs.metanode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.infolink.dfs.metanode.event.DeadNodeEvent;
import com.infolink.dfs.metanode.mdb.BlockNode;
import com.infolink.dfs.shared.DfsNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class DeadNodeEventListener {
    private static final Logger logger = LoggerFactory.getLogger(DeadNodeEventListener.class);
    private static final int BLOCK_NODE_LIMIT = 1000; // Limit for the number of block nodes to process
    private static final String BLOCK_NODE_PREFIX = BlockMetaService.BLOCK_NODE_PREFIX; // Prefix for block nodes in Redis
    
    @Autowired
    private RedisTemplate<String, BlockNode> redisTemplate;
    
    @Autowired
    private NodeManager nodeManager;
    
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private ObjectMapper objectMapper;    
    public DeadNodeEventListener() {
    }

    @EventListener
    public void handleDeadNodeEvent(DeadNodeEvent event) {
        DfsNode deadNode = event.getDeadNode();
        logger.info("Processing dead node: {}", deadNode.getContainerUrl());

        ScanOptions scanOptions = ScanOptions.scanOptions().match(BLOCK_NODE_PREFIX+"*").count(BLOCK_NODE_LIMIT).build();

        try (@SuppressWarnings("deprecation")
		Cursor<byte[]> cursor = redisTemplate.executeWithStickyConnection(
                redisConnection -> redisConnection.scan(scanOptions))) {

            int count = 0;

            while (cursor.hasNext()) {
                byte[] keyBytes = cursor.next();
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                
                BlockNode blockNode = redisTemplate.opsForValue().get(key);

                // Check if the block node contains the dead node URL
                Set<String> nodeUrls = blockNode.getNodeUrls();
                if (nodeUrls.contains(deadNode.getContainerUrl())) {
                    logger.info("Replicating block {} to a living node.", blockNode.getHash());
                    nodeUrls.remove(deadNode.getContainerUrl());
                    
                    replicateBlockToLivingNode(nodeUrls, blockNode.getHash());
                }
                
                count++;

                // After processing 1000 records, sleep and check if node is still dead
                if (count == BLOCK_NODE_LIMIT) {
                    try {
                        Thread.sleep(10000); // Adjust sleep time as needed
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // Restore interrupt status
                    }

                    // Check if the dead node is still inactive
                    if (!nodeManager.isDeadNode(deadNode)) {
                        break; // Exit the loop if the node is no longer dead
                    }
                    count = 0; // Reset count for the next batch
                }
            }
        }        
        logger.info("All BlockNode records processed for dead node: {}", deadNode.getContainerUrl());
    }
    
    private boolean replicateBlockToLivingNode(Set<String> existingNodeUrls, String blockHash) {
        DfsNode newNode = findLivingNode(existingNodeUrls);
        if (newNode == null) return false;

        String targetNodeUrl = existingNodeUrls.iterator().next();
        String url = targetNodeUrl + "/dfs/block/replicate-to-another-node";

        try {
            String nodeJson = objectMapper.writeValueAsString(newNode);
            
            Map<String, Object> request = new HashMap<>();
            request.put("blockHash", blockHash);
            request.put("targetNode", nodeJson);  // Pass JSON string here

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

            ResponseEntity<String> response = restTemplate.exchange(
                url, HttpMethod.POST, entity, String.class);

            logger.info("Replicating block {} to node {}. Response: {}", blockHash, newNode.getContainerUrl(), response.getBody());
            return response.getStatusCode().is2xxSuccessful();

        } catch (Exception e) {
            logger.error("Error serializing DfsNode or sending request: ", e);
            return false;
        }
    }
    
	private DfsNode findLivingNode(Set<String> existingNodeUrls) {
        logger.debug("File a living node for block replication. ");
        logger.debug("The existing node for a block are: {}", existingNodeUrls);
        
		List<DfsNode> livingNodes = nodeManager.getRegisteredNodes();
		
		logger.debug("Living nodes are: ");
		for(DfsNode node : livingNodes) {
			logger.debug("     {}", node.getContainerUrl());
		}
		
        for (DfsNode node : livingNodes) {
            if (!existingNodeUrls.contains(node.getContainerUrl())) {
            	logger.debug("Pick the living node({}) and return.", node.getContainerUrl());
                return node; // Return the first living node found
            }
        }
        return null; // No living nodes available
    }

}
