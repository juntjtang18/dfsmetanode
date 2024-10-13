package com.fdu.msacs.dfs.metanode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.*;
@SpringBootTest
public class MetaNodeServiceTest {
    @Autowired
    private MetaNodeService metaNode;

    @BeforeEach
    public void setUp() throws IOException {

    	metaNode.clearCache();
    	metaNode.clearRegisteredNodes();
    }

    @Test
    public void testRegisterNode() {
        String nodeAddress = "http://node1.com";
        String response = metaNode.registerNode(nodeAddress);
        assertEquals("Node registered: " + nodeAddress, response);

        // Test registering the same node again
        response = metaNode.registerNode(nodeAddress);
        assertEquals("Node already registered: " + nodeAddress, response);
    }

    @Test
    public void testRegisterFileLocation() {
        String filename = "testfile.txt";
        String nodeUrl = "http://node1.com";
        metaNode.registerNode(nodeUrl);

        String response = metaNode.registerFileLocation(filename, nodeUrl);
        assertEquals("File location registered: " + filename + " on " + nodeUrl, response);

        // Verify that the file is associated with the node
        List<String> nodes = metaNode.getNodesForFile(filename);
        assertTrue(nodes.contains(nodeUrl));
    }

    @Test
    public void testGetNodesForFile() {
        String filename = "testfile.txt";
        String nodeUrl1 = "http://node1.com";
        String nodeUrl2 = "http://node2.com";
        metaNode.registerNode(nodeUrl1);
        metaNode.registerNode(nodeUrl2);

        metaNode.registerFileLocation(filename, nodeUrl1);
        metaNode.registerFileLocation(filename, nodeUrl2);

        List<String> nodes = metaNode.getNodesForFile(filename);
        assertTrue(nodes.contains(nodeUrl1));
        assertTrue(nodes.contains(nodeUrl2));
    }

    @Test
    public void testGetReplicationNodes() {
        String filename = "testfile.txt";
        String requestingNodeUrl = "http://node1.com";
        String nodeUrl2 = "http://node2.com";
        metaNode.registerNode(requestingNodeUrl);
        metaNode.registerNode(nodeUrl2);

        metaNode.registerFileLocation(filename, requestingNodeUrl);

        List<String> replicationNodes = metaNode.getReplicationNodes(filename, requestingNodeUrl);
        assertTrue(replicationNodes.contains(nodeUrl2));
        assertFalse(replicationNodes.contains(requestingNodeUrl));
    }

    @Test
    public void testSelectNodeForUpload() {
        String nodeUrl1 = "http://node1.com";
        String nodeUrl2 = "http://node2.com";
        metaNode.registerNode(nodeUrl1);
        metaNode.registerNode(nodeUrl2);

        String selectedNode1 = metaNode.selectNodeForUpload();
        String selectedNode2 = metaNode.selectNodeForUpload();

        assertNotNull(selectedNode1);
        assertNotNull(selectedNode2);
        assertNotEquals(selectedNode1, selectedNode2);
    }

    @Test
    public void testClearCache() {
        String filename = "testfile.txt";
        String nodeUrl = "http://node1.com";
        metaNode.registerNode(nodeUrl);
        metaNode.registerFileLocation(filename, nodeUrl);

        metaNode.clearCache();

        List<String> nodes = metaNode.getNodesForFile(filename);
        assertTrue(nodes.isEmpty());
    }

    @Test
    public void testClearRegisteredNodes() {
        String nodeUrl = "http://node1.com";
        metaNode.registerNode(nodeUrl);

        metaNode.clearRegisteredNodes();

        assertTrue(metaNode.getRegisteredNodes().isEmpty());
    }
}
