package com.infolink.dfs.metanode;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;

import com.infolink.dfs.shared.DfsNode;
import com.infolink.dfs.metanode.event.DeadNodeEvent;

@SpringBootTest
public class NodeManagerTest2 {
	
	@Autowired
    private NodeManager nodeManager;

    @Autowired
    ApplicationEventPublisher eventPublisher;
    
    @BeforeEach
    public void setUp() {

    }

    @Test
    public void testCheckNodeHealth_MovesDeadNodes() {
        // Create a DfsNode with a last report time
    	String node1 = "node1";
    	String node2 = "node2";
        DfsNode activeNode = new DfsNode(node1, node1);
        DfsNode deadNode = new DfsNode(node2, node2);
        deadNode.setLastTimeReport(new Date(System.currentTimeMillis()-30000));
        
        // Register nodes
        nodeManager.registerNode(activeNode);
        nodeManager.registerNode(deadNode);

        // Perform health check
        nodeManager.checkNodeHealth();

        // Assert that the active node is moved to dead nodes
        //assertTrue(nodeManager.getDeadNodes().containsKey(node2));
        //assertFalse(nodeManager.getDeadNodes().containsKey(node1));

    }

    //@Test
    public void testCheckNodeHealth_NoDeadNodes() {
        // Create a DfsNode that is healthy
        DfsNode healthyNode = mock(DfsNode.class);
        when(healthyNode.getContainerUrl()).thenReturn("http://healthy-node.com");
        when(healthyNode.getLastTimeReport()).thenReturn(new Date(System.currentTimeMillis())); // Last report just now

        // Register the healthy node
        nodeManager.registerNode(healthyNode);

        // Perform health check
        nodeManager.checkNodeHealth();

        // Assert that no nodes are in dead nodes
        //assertFalse(nodeManager.getDeadNodes().containsKey("http://healthy-node.com"));
    }
}
