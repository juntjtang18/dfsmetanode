package com.infolink.dfs.metanode;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.infolink.dfs.metanode.BlockMetaController.ResponseNodesForBlock;
import com.infolink.dfs.shared.DfsNode;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class NodeManagerTest {
	@Autowired
    private NodeManager nodeManager;

    @BeforeEach
    public void setUp() {
    	nodeManager.clearRegisteredNodes();
    }

    @Test
    public void testRegisterNode() {
        DfsNode node = createDfsNode("http://node1.com", "http://local1.com");
        String response = nodeManager.registerNode(node);
        
        assertEquals("Node registered: http://node1.com", response);
        assertEquals(1, nodeManager.getRegisteredNodes().size());
        
        // Test reviving a dead node
        //String nodeUrl =node.getContainerUrl(); 
        //nodeManager.getDeadNodes().put(nodeUrl, node);
        //nodeManager.getRegisteredNodes().remove(nodeUrl);
        //response = nodeManager.registerNode(node);
        //assertEquals("A dead node revives: http://node1.com", response);
        //assertEquals(1, nodeManager.getRegisteredNodes().size());
    }

    @Test
    public void testGetReplicationNodes() {
        DfsNode node1 = createDfsNode("http://node1.com", "http://local1.com");
        DfsNode node2 = createDfsNode("http://node2.com", "http://local2.com");
        DfsNode node3 = createDfsNode("http://node3.com", "http://local3.com");
        
        nodeManager.registerNode(node1);
        nodeManager.registerNode(node2);
        nodeManager.registerNode(node3);

        List<DfsNode> replicationNodes = nodeManager.getReplicationNodes("testfile.txt", "http://node1.com");
        assertEquals(2, replicationNodes.size());
        assertTrue(replicationNodes.contains(node2) || replicationNodes.contains(node3));
    }

    @Test
    public void testSelectNodeBasedOnBlockCount() {
        DfsNode node1 = createDfsNode("http://node1.com", "http://local1.com");
        node1.setBlockCount(1);
        DfsNode node2 = createDfsNode("http://node2.com", "http://local2.com");
        node2.setBlockCount(3);
        DfsNode node3 = createDfsNode("http://node3.com", "http://local3.com");
        node3.setBlockCount(2);
        
        nodeManager.registerNode(node1);
        nodeManager.registerNode(node2);
        nodeManager.registerNode(node3);
        
        Set<String> existingNodeUrls = new HashSet<>();
        existingNodeUrls.add("http://node1.com");

        ResponseNodesForBlock response = nodeManager.selectNodeBasedOnBlockCount(existingNodeUrls, 3, "http://node1.com");
        assertEquals(ResponseNodesForBlock.Status.SUCCESS, response.getStatus());
        assertEquals(2, response.getNodes().size()); // Should select node2 and node3

        // Test with enough copies
        existingNodeUrls.add("http://node2.com");
        existingNodeUrls.add("http://node3.com");
        response = nodeManager.selectNodeBasedOnBlockCount(existingNodeUrls, 3, "http://node1.com");
        assertEquals(ResponseNodesForBlock.Status.ALREADY_ENOUGH_COPIES, response.getStatus());
    }

    //@Test
    public void testCheckNodeHealth() throws InterruptedException {
        DfsNode node = createDfsNode("http://node1.com", "http://local1.com");
        nodeManager.registerNode(node);
        
        // Simulate heartbeat
        node.setLastTimeReport(new Date());
        Thread.sleep(31000); // Wait for health check to kick in
        
        nodeManager.checkNodeHealth();
        assertTrue(nodeManager.getDeadNodes().contains(node)); // Should move to deadNodes
        assertFalse(nodeManager.getRegisteredNodes().contains(node)); // Should be removed from registeredNodes
    }

    private DfsNode createDfsNode(String containerUrl, String localUrl) {
        DfsNode node = new DfsNode();
        node.setContainerUrl(containerUrl);
        node.setLocalUrl(localUrl);
        node.setLastTimeReport(new Date());
        node.setBlockCount(0); // Initialize with 0 blocks for testing
        node.setBlockTotalSize(0); // Initialize size
        return node;
    }
}
