package com.fdu.msacs.dfs.metanode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fdu.msacs.dfs.metanode.meta.DfsNode;

public class NodeManager_RegisterNodeTest {
    private NodeManager nodeManager;

    @BeforeEach
    public void setUp() {
        nodeManager = new NodeManager();
    }

    @Test
    public void testRegisterNewNode() {
        DfsNode newNode = new DfsNode("http://node1:8080", new Date());
        
        String result = nodeManager.registerNode(newNode);
        
        assertEquals("Node registered: http://node1:8080", result);
        assertTrue(nodeManager.getRegisteredNodes().contains(newNode));
        assertEquals(1, nodeManager.getRegisteredNodes().size());
    }

    @Test
    public void testRegisterExistingNodeHeartbeat() {
        DfsNode existingNode = new DfsNode("http://node2:8080", new Date());
        nodeManager.registerNode(existingNode);

        // Simulate heartbeat by updating the node's last report time.
        String result = nodeManager.registerNode(existingNode);

        assertEquals("Received Heartbeat from http://node2:8080", result);
        assertEquals(1, nodeManager.getRegisteredNodes().size());
        assertTrue(nodeManager.getRegisteredNodes().contains(existingNode));
    }

    @Test
    public void testReviveDeadNode() {
        DfsNode deadNode = new DfsNode("http://node3:8080", new Date());
        nodeManager.registerNode(deadNode);

        // Simulate node becoming dead.
        nodeManager.getDeadNodes().put(deadNode.getContainerUrl(), deadNode);
        nodeManager.getRegisteredNodes().remove(deadNode.getContainerUrl());

        // Simulate the node reporting again (reviving).
        String result = nodeManager.registerNode(deadNode);

        assertEquals("A dead node revives: http://node3:8080", result);
        assertTrue(nodeManager.getRegisteredNodes().contains(deadNode));
        assertEquals(0, nodeManager.getDeadNodes().size());
    }

    @Test
    public void testRegisterDeadNodeAsNew() {
        DfsNode newNode = new DfsNode("http://node4:8080", new Date());
        
        // Simulate the node being dead initially.
        nodeManager.getDeadNodes().put(newNode.getContainerUrl(), newNode);

        // Register the same node again, it should revive.
        String result = nodeManager.registerNode(newNode);

        assertEquals("A dead node revives: http://node4:8080", result);
        assertTrue(nodeManager.getRegisteredNodes().contains(newNode));
        assertEquals(0, nodeManager.getDeadNodes().size());
    }

    @Test
    public void testRegisterMultipleNodes() {
        DfsNode node1 = new DfsNode("http://node5:8080", new Date());
        DfsNode node2 = new DfsNode("http://node6:8080", new Date());

        nodeManager.registerNode(node1);
        nodeManager.registerNode(node2);

        assertEquals(2, nodeManager.getRegisteredNodes().size());
        assertTrue(nodeManager.getRegisteredNodes().contains(node1));
        assertTrue(nodeManager.getRegisteredNodes().contains(node2));
    }
}
