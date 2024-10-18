package com.fdu.msacs.dfs.metanode;

import com.fdu.msacs.dfs.metanode.meta.DfsNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class NodeManagerSelectNodeRoundRobinTest {

    private NodeManager nodeManager;

    @BeforeEach
    void setUp() {
        nodeManager = new NodeManager();
        nodeManager.clearRegisteredNodes();
    }

    @Test
    void testSelectNodeRoundRobin_withNoRegisteredNodes() {
        // Case 1: No registered nodes, expecting an empty list.
        List<DfsNode> result = nodeManager.selectNodeRoundRobin(Collections.emptySet(), 2, "http://requestingNode");
        assertTrue(result.isEmpty(), "Expected an empty list when no registered nodes are available.");
    }

    @Test
    void testSelectNodeRoundRobin_withExistingNodesOnly() {
        // Case 2: Only existing nodes, expecting an empty list as all needed nodes are already present.
        DfsNode node1 = new DfsNode("http://node1");
        nodeManager.registerNode(node1);

        Set<String> existingNodes = new HashSet<>();
        existingNodes.add("http://node1");

        List<DfsNode> result = nodeManager.selectNodeRoundRobin(existingNodes, 1, "http://node1");
        assertTrue(result.isEmpty(), "Expected an empty list when all requested nodes are already in the existing set.");
    }

    @Test
    void testSelectNodeRoundRobin_withRequestingNodeAdded() {
        // Case 3: Requesting node is not in existingNodes, should be added to the result.
        DfsNode node1 = new DfsNode("http://node1");
        nodeManager.registerNode(node1);

        Set<String> existingNodes = new HashSet<>();
        List<DfsNode> result = nodeManager.selectNodeRoundRobin(existingNodes, 1, "http://node1");

        assertEquals(1, result.size(), "Expected the requesting node to be selected.");
        assertEquals("http://node1", result.get(0).getContainerUrl(), "Expected the requesting node to be in the result.");
    }

    @Test
    void testSelectNodeRoundRobin_withMoreNodesThanRequested() {
        // Case 4: Registered nodes exceed the requested count.
        DfsNode node1 = new DfsNode("http://node1");
        DfsNode node2 = new DfsNode("http://node2");
        DfsNode node3 = new DfsNode("http://node3");
        nodeManager.registerNode(node1);
        nodeManager.registerNode(node2);
        nodeManager.registerNode(node3);

        Set<String> existingNodes = new HashSet<>();
        List<DfsNode> result = nodeManager.selectNodeRoundRobin(existingNodes, 2, "http://node1");

        assertEquals(2, result.size(), "Expected two nodes to be selected.");
        assertTrue(result.contains(node1), "Selected nodes should include the requesting node if it's not in the existing set.");
        assertTrue(result.stream().allMatch(node -> !existingNodes.contains(node.getContainerUrl())),
                   "Selected nodes should not include those in the existing set.");
    }



    @Test
    void testSelectNodeRoundRobin_withRoundRobinSelection() {
        // Case 5: More nodes registered than requested, testing round-robin behavior.
        DfsNode node1 = new DfsNode("http://node1");
        DfsNode node2 = new DfsNode("http://node2");
        DfsNode node3 = new DfsNode("http://node3");
        nodeManager.registerNode(node1);
        nodeManager.registerNode(node2);
        nodeManager.registerNode(node3);

        Set<String> existingNodes = new HashSet<>();
        existingNodes.add(node1.getContainerUrl());
        List<DfsNode> result1 = nodeManager.selectNodeRoundRobin(existingNodes, 2, "http://node1");
        List<DfsNode> result2 = nodeManager.selectNodeRoundRobin(existingNodes, 2, "http://node1");

        assertNotEquals(result1.get(0), result2.get(0), "Expected different nodes for successive round-robin calls.");
    }

    @Test
    void testSelectNodeRoundRobin_withInsufficientRegisteredNodes() {
        // Case 6: More nodes requested than available after excluding existing nodes.
        DfsNode node1 = new DfsNode("http://node1");
        DfsNode node2 = new DfsNode("http://node2");
        nodeManager.registerNode(node1);
        nodeManager.registerNode(node2);

        Set<String> existingNodes = new HashSet<>();
        existingNodes.add("http://node1");

        List<DfsNode> result = nodeManager.selectNodeRoundRobin(existingNodes, 3, "http://node1");

        assertEquals(1, result.size(), "Expected only one new node to be selected.");
        assertEquals("http://node2", result.get(0).getContainerUrl());
    }

    @Test
    void testSelectNodeRoundRobin_withExistingAndNewNodes() {
        // Case 7: Mix of existing and new nodes.
        DfsNode node1 = new DfsNode("http://node1");
        DfsNode node2 = new DfsNode("http://node2");
        nodeManager.registerNode(node1);
        nodeManager.registerNode(node2);

        Set<String> existingNodes = new HashSet<>();
        existingNodes.add("http://node1");

        List<DfsNode> result = nodeManager.selectNodeRoundRobin(existingNodes, 2, "http://node2");

        assertEquals(1, result.size(), "Expected only one new node to be selected.");
        assertEquals("http://node2", result.get(0).getContainerUrl());
    }

    @Test
    void testSelectNodeRoundRobin_withAllNodesInExisting() {
        // Case 8: All registered nodes are in the existing set.
        DfsNode node1 = new DfsNode("http://node1");
        DfsNode node2 = new DfsNode("http://node2");
        nodeManager.registerNode(node1);
        nodeManager.registerNode(node2);

        Set<String> existingNodes = new HashSet<>();
        existingNodes.add("http://node1");
        existingNodes.add("http://node2");

        List<DfsNode> result = nodeManager.selectNodeRoundRobin(existingNodes, 2, "http://node1");

        assertTrue(result.isEmpty(), "Expected an empty list since all nodes are already in the existing set.");
    }
}
