package com.fdu.msacs.dfs.metanode;

import com.fdu.msacs.dfs.metanode.mdb.BlockNode;
import com.fdu.msacs.dfs.metanode.mdb.BlockNodeMappingRepo;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ComponentScan(basePackages = "com.fdu.msacs.dfs.metanode")
public class BlockMetaServiceTest {

    @Autowired
    private BlockNodeMappingRepo blockNodeMappingRepo;

    @Autowired
    private BlockMetaService blockService;

    @Autowired
    private NodeManager nodeManager;

    @Autowired
    private AppConfig appConfig; // Inject AppConfig to access replication factor

    @BeforeEach
    void setUp() {
        // Clean up the database before each test.
    	nodeManager.clearRegisteredNodes();
        blockNodeMappingRepo.deleteAll();
    }

    @Test
    void testRegisterBlockLocation() {
        String hash = "hash1";
        String nodeUrl = "http://node1.com";

        String response = blockService.registerBlockLocation(hash, nodeUrl);
        assertEquals("Block location registered: hash1 on http://node1.com", response);

        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        assertNotNull(blockNode);
        assertTrue(blockNode.getNodeUrls().contains(nodeUrl));
    }

    @Test
    void testBlockExists() {
        String hash = "hash2";
        String nodeUrl1 = "http://node2.com";
        String nodeUrl2 = "http://node3.com";

        blockService.registerBlockLocation(hash, nodeUrl1);
        blockService.registerBlockLocation(hash, nodeUrl2);

        Set<String> nodeUrls = blockService.blockExists(hash);
        assertNotNull(nodeUrls);
        assertEquals(2, nodeUrls.size());
        assertTrue(nodeUrls.contains(nodeUrl1));
        assertTrue(nodeUrls.contains(nodeUrl2));
    }

    @Test
    void testBlockDoesNotExist() {
        Set<String> nodeUrls = blockService.blockExists("nonexistentHash");
        assertNull(nodeUrls);
    }

    @Test
    void testUnregisterBlock() {
        String hash = "hash3";
        String nodeUrl = "http://node4.com";

        blockService.registerBlockLocation(hash, nodeUrl);
        String response = blockService.unregisterBlock(hash);
        assertEquals("Block unregistered: hash3", response);

        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        assertNull(blockNode);
    }

    @Test
    void testUnregisterBlockFromNode() {
        String hash = "hash4";
        String nodeUrl1 = "http://node5.com";
        String nodeUrl2 = "http://node6.com";

        blockService.registerBlockLocation(hash, nodeUrl1);
        blockService.registerBlockLocation(hash, nodeUrl2);

        String response = blockService.unregisterBlockFromNode(hash, nodeUrl1);
        assertEquals("Node URL removed from block: http://node5.com for hash: hash4", response);

        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        assertNotNull(blockNode);
        assertFalse(blockNode.getNodeUrls().contains(nodeUrl1));
        assertTrue(blockNode.getNodeUrls().contains(nodeUrl2));
    }

    @Test
    void testUnregisterBlockFromNodeAndDeleteBlock() {
        String hash = "hash5";
        String nodeUrl = "http://node7.com";

        blockService.registerBlockLocation(hash, nodeUrl);
        String response = blockService.unregisterBlockFromNode(hash, nodeUrl);
        assertEquals("Block fully unregistered as no node URLs remain: hash5", response);

        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        assertNull(blockNode);
    }

    @Test
    void testUnregisterBlockFromNodeWhenNodeNotFound() {
        String hash = "hash6";
        String nodeUrl = "http://node8.com";

        blockService.registerBlockLocation(hash, nodeUrl);

        String response = blockService.unregisterBlockFromNode(hash, "http://nonexistentNode.com");
        assertEquals("Node URL: http://nonexistentNode.com not found for block with hash: hash6", response);

        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        assertNotNull(blockNode);
        assertTrue(blockNode.getNodeUrls().contains(nodeUrl));
    }
    @Test
    void testClearAllBlockNodes() {
        // Setup initial data
        blockService.registerBlockLocation("hash1", "http://node1.com");
        blockService.registerBlockLocation("hash2", "http://node2.com");
        blockService.registerBlockLocation("hash3", "http://node3.com");

        // Assert that records are present before clearing
        assertEquals(3, blockNodeMappingRepo.findAll().size(), "Should have 3 block nodes before clearing.");

        // Call the method to clear all block nodes
        blockService.clearAllBlockNodes();

        // Assert that all records are cleared
        assertEquals(0, blockNodeMappingRepo.findAll().size(), "Should have 0 block nodes after clearing.");
    }
    
    @Test
    void testCheckReplicationAndSelectNodesNotInRegistered() {
        String hash = "hash1";
        String requestingNodeUrl = "http://requestingNode.com";

        // Register N nodes
        String nodeUrl1 = "http://node1.com";
        String nodeUrl2 = "http://node2.com";
        String nodeUrl3 = "http://node3.com";
        int replicationFactor = appConfig.getReplicationFactor(); // Get replication factor from AppConfig

        // Register the block with 2 nodes
        blockService.registerBlockLocation(hash, nodeUrl1);
        blockService.registerBlockLocation(hash, nodeUrl2);

        // Assume nodeManager is set up correctly with these nodes
        nodeManager.registerNode(new DfsNode(nodeUrl1));
        nodeManager.registerNode(new DfsNode(nodeUrl2));
        nodeManager.registerNode(new DfsNode(nodeUrl3));
        nodeManager.registerNode(new DfsNode(requestingNodeUrl)); // Include requesting node

        List<DfsNode> selectedNodes = blockService.checkReplicationAndSelectNodes(hash, requestingNodeUrl);

        // Should include the requesting node and replicationFactor - 1 nodes from registered nodes
        assertEquals(1, selectedNodes.size());
        assertTrue(selectedNodes.stream().anyMatch(node -> node.getContainerUrl().equals(requestingNodeUrl)));
    }

    @Test
    void testCheckReplicationAndSelectNodesInRegistered() {
        String hash = "hash2";
        String requestingNodeUrl = "http://node1.com"; // This is part of the registered nodes

        // Register N nodes
        String nodeUrl1 = "http://node1.com";
        String nodeUrl2 = "http://node2.com";
        int replicationFactor = appConfig.getReplicationFactor(); // Get replication factor from AppConfig

        // Register the block with 1 node
        blockService.registerBlockLocation(hash, nodeUrl1);
        blockService.registerBlockLocation(hash, requestingNodeUrl);

        // Assume nodeManager is set up correctly with these nodes
        nodeManager.registerNode(new DfsNode(nodeUrl1));
        nodeManager.registerNode(new DfsNode(nodeUrl2));
        nodeManager.registerNode(new DfsNode(requestingNodeUrl));

        List<DfsNode> selectedNodes = blockService.checkReplicationAndSelectNodes(hash, requestingNodeUrl);

        // Should not include the requesting node and should return replicationFactor - M nodes from registered nodes
        assertEquals(1, selectedNodes.size());
        assertFalse(selectedNodes.stream().anyMatch(node -> node.getContainerUrl().equals(requestingNodeUrl)));
        assertTrue(selectedNodes.stream().anyMatch(node -> node.getContainerUrl().equals(nodeUrl2)));
    }
}
