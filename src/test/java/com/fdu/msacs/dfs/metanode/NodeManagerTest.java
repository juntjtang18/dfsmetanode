package com.fdu.msacs.dfs.metanode;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fdu.msacs.dfs.metanode.meta.DfsNode;

public class NodeManagerTest {

    @InjectMocks
    private NodeManager nodeManager;

    @Mock
    private DfsNode dfsNode1;
    
    @Mock
    private DfsNode dfsNode2;
    
    @Mock
    private DfsNode dfsNode3;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(dfsNode1.getContainerUrl()).thenReturn("http://node1");
        when(dfsNode2.getContainerUrl()).thenReturn("http://node2");
        when(dfsNode3.getContainerUrl()).thenReturn("http://node3");
        
    }

    @Test
    public void testRegisterNode_NewNode() {
        String result = nodeManager.registerNode(dfsNode1);
        assertEquals("Node registered: http://node1", result);
        assertTrue(nodeManager.getRegisteredNodes().contains(dfsNode1));
    }

    @Test
    public void testRegisterNode_DeadNodeRevives() {
        nodeManager.registerNode(dfsNode1); // Register first time
        nodeManager.clearRegisteredNodes(); // Clear for simulation

        // Simulate dead node by adding it to deadNodes
        nodeManager.getDeadNodes().put(dfsNode1.getContainerUrl(), dfsNode1);
        String result = nodeManager.registerNode(dfsNode1);
        assertEquals("A dead node revives: http://node1", result);
        assertTrue(nodeManager.getRegisteredNodes().contains(dfsNode1));
    }

    @Test
    public void testGetReplicationNodes() {
        // Register nodes
        nodeManager.registerNode(dfsNode1);  // Requesting node
        nodeManager.registerNode(dfsNode2);  // Live node
        nodeManager.registerNode(dfsNode3);  // Live node

        // Request replication nodes from "http://node1"
        List<DfsNode> nodes = nodeManager.getReplicationNodes("file.txt", "http://node1");

        // Since there are two other live nodes, the method should return both.
        assertEquals(2, nodes.size(), "Expected 2 nodes to be returned.");
        assertTrue(nodes.contains(dfsNode2), "Should contain dfsNode2.");
        assertTrue(nodes.contains(dfsNode3), "Should contain dfsNode3.");
        assertFalse(nodes.stream().anyMatch(node -> node.getContainerUrl().equals("http://node1")),
                "Should not contain the requesting node (http://node1).");
    }


    @Test
    public void testSelectNodeForUploadWithThreeNodes() {
        // Register three nodes
        nodeManager.registerNode(dfsNode1);
        nodeManager.registerNode(dfsNode2);
        nodeManager.registerNode(dfsNode3);
        
        // Create a Set to hold selected nodes for verification
        Set<DfsNode> selectedNodes = new HashSet<>();

        // Call selectNodeForUpload three times
        for (int i = 0; i < 3; i++) {
            DfsNode selectedNode = nodeManager.selectNodeForUpload();
            selectedNodes.add(selectedNode);
        }

        // Check that all three nodes have been selected
        assertEquals(3, selectedNodes.size()); // Should have all 3 nodes in the set
        assertTrue(selectedNodes.contains(dfsNode1)); // Should contain dfsNode1
        assertTrue(selectedNodes.contains(dfsNode2)); // Should contain dfsNode2
        assertTrue(selectedNodes.contains(dfsNode3)); // Should contain dfsNode3
    }


    @Test
    public void testCheckNodeHealth_HealthyNode() {
        nodeManager.registerNode(dfsNode1);
        when(dfsNode1.getLastTimeReport()).thenReturn(new Date()); // Set last report to now

        nodeManager.checkNodeHealth();
    }

    @Test
    public void testCheckNodeHealth_DeadNode() throws InterruptedException {
        nodeManager.registerNode(dfsNode1);
        when(dfsNode1.getLastTimeReport()).thenReturn(new Date(System.currentTimeMillis() - 7000)); // Set last report to 7 seconds ago

        nodeManager.checkNodeHealth();

        // Expect node to be moved to deadNodes
        assertTrue(nodeManager.getDeadNodes().containsKey(dfsNode1.getContainerUrl()));
        assertFalse(nodeManager.getRegisteredNodes().contains(dfsNode1));
    }

    @Test
    public void testClearRegisteredNodes() {
        nodeManager.registerNode(dfsNode1);
        nodeManager.clearRegisteredNodes();
        
        assertTrue(nodeManager.getRegisteredNodes().isEmpty());
        assertTrue(nodeManager.getDeadNodes().isEmpty());
    }
}
