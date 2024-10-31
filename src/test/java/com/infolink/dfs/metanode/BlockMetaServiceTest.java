package com.infolink.dfs.metanode;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.infolink.dfs.metanode.mdb.BlockNode;

import java.util.Set;

@SpringBootTest
@ActiveProfiles("test")
public class BlockMetaServiceTest {

    @Autowired
    private BlockMetaService blockMetaService;

    private final String TEST_HASH_1 = "hash1";
    private final String TEST_HASH_2 = "hash2";
    private final String TEST_NODE_URL_1 = "http://node1.com";
    private final String TEST_NODE_URL_2 = "http://node2.com";

    @BeforeEach
    public void setup() {
        blockMetaService.clearAllBlockNodes(); // Clear any existing data before each test
    }

    @Test
    public void testRegisterBlockLocation() {
        String response = blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_1);
        assertThat(response).contains("Block location registered: hash1 on http://node1.com");

        // Register another node for the same hash
        response = blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_2);
        assertThat(response).contains("Block location registered: hash1 on http://node2.com");

        // Verify the block node mapping
        BlockNode blockNode = blockMetaService.getBlockNodeByHash(TEST_HASH_1);
        assertThat(blockNode).isNotNull();
        assertThat(blockNode.getNodeUrls()).containsExactlyInAnyOrder(TEST_NODE_URL_1, TEST_NODE_URL_2);
    }

    @Test
    public void testGetBlockNodeByHash() {
        // Register multiple node locations for the same hash
        blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_1);
        blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_2);
        blockMetaService.registerBlockLocation(TEST_HASH_1, "http://node3.com");
        blockMetaService.registerBlockLocation(TEST_HASH_1, "http://node4.com");

        // Retrieve the block node by hash
        BlockNode blockNode = blockMetaService.getBlockNodeByHash(TEST_HASH_1);
        
        // Verify the block node is not null and contains the correct hash
        assertThat(blockNode).isNotNull();
        assertThat(blockNode.getHash()).isEqualTo(TEST_HASH_1);
        
        // Verify that all registered node URLs are present
        assertThat(blockNode.getNodeUrls()).containsExactlyInAnyOrder(TEST_NODE_URL_1, TEST_NODE_URL_2, "http://node3.com", "http://node4.com");
    }

    @Test
    public void testBlockExists() {
        blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_1);
        Set<String> existingNodes = blockMetaService.blockExists(TEST_HASH_1);
        
        assertThat(existingNodes).isNotNull();
        assertThat(existingNodes).contains(TEST_NODE_URL_1);
        
        // Check a non-existent block
        existingNodes = blockMetaService.blockExists(TEST_HASH_2);
        assertThat(existingNodes).isNull();
    }

    @Test
    public void testUnregisterBlock() {
        blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_1);
        String response = blockMetaService.unregisterBlock(TEST_HASH_1);
        assertThat(response).contains("Block unregistered: hash1");

        // Verify that the block is unregistered
        BlockNode blockNode = blockMetaService.getBlockNodeByHash(TEST_HASH_1);
        assertThat(blockNode).isNull();
    }

    @Test
    public void testUnregisterBlockFromNode() {
        blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_1);
        blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_2);
        
        String response = blockMetaService.unregisterBlockFromNode(TEST_HASH_1, TEST_NODE_URL_1);
        assertThat(response).contains("Node URL removed from block: http://node1.com for hash: hash1");

        // Verify the remaining nodes
        BlockNode blockNode = blockMetaService.getBlockNodeByHash(TEST_HASH_1);
        assertThat(blockNode.getNodeUrls()).containsExactly(TEST_NODE_URL_2);
        
        // Unregister the second node and verify complete unregistration
        response = blockMetaService.unregisterBlockFromNode(TEST_HASH_1, TEST_NODE_URL_2);
        assertThat(response).contains("Block fully unregistered as no node URLs remain: hash1");
        assertThat(blockMetaService.getBlockNodeByHash(TEST_HASH_1)).isNull();
    }

    @Test
    public void testClearAllBlockNodes() {
        blockMetaService.registerBlockLocation(TEST_HASH_1, TEST_NODE_URL_1);
        blockMetaService.registerBlockLocation(TEST_HASH_2, TEST_NODE_URL_2);

        String response = blockMetaService.clearAllBlockNodes();
        assertThat(response).contains("All block nodes have been cleared.");

        // Verify that all blocks are cleared
        assertThat(blockMetaService.getBlockNodeByHash(TEST_HASH_1)).isNull();
        assertThat(blockMetaService.getBlockNodeByHash(TEST_HASH_2)).isNull();
    }
}
