package com.fdu.msacs.dfs.metanode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)

public class MetaNodeServiceTest {
	@Autowired
    private MetaNodeService metaNodeService;

    @BeforeEach
    public void setUp() {
        metaNodeService.clearCache();
        metaNodeService.clearRegisteredNodes();
    }

    @Test
    public void testRegisterNode() {
        DfsNode node = new DfsNode("http://localhost:8081");
        String response = metaNodeService.registerNode(node);
        
        assertEquals("Node registered: http://localhost:8081", response);
        
        // Registering the same node again
        response = metaNodeService.registerNode(node);
        assertEquals("Node already registered: http://localhost:8081", response);
    }

    @Test
    public void testRegisterFileLocation() {
        DfsNode node = new DfsNode("http://localhost:8081");
        metaNodeService.registerNode(node);
        
        String response = metaNodeService.registerFileLocation("testFile.txt", node.getNodeUrl());
        assertEquals("File location registered: testFile.txt on http://localhost:8081", response);
        
        // Registering the file location again
        response = metaNodeService.registerFileLocation("testFile.txt", node.getNodeUrl());
        assertEquals("File location registered: testFile.txt on http://localhost:8081", response);
    }

    @Test
    public void testGetNodesForFile() {
        DfsNode node1 = new DfsNode("http://localhost:8081");
        DfsNode node2 = new DfsNode("http://localhost:8082");
        metaNodeService.registerNode(node1);
        metaNodeService.registerNode(node2);
        
        metaNodeService.registerFileLocation("testFile.txt", node1.getNodeUrl());
        metaNodeService.registerFileLocation("testFile.txt", node2.getNodeUrl());

        List<String> nodes = metaNodeService.getNodesForFile("testFile.txt");
        
        assertThat(nodes).containsExactlyInAnyOrder(node1.getNodeUrl(), node2.getNodeUrl());
    }

    @Test
    public void testGetReplicationNodes() {
        DfsNode node1 = new DfsNode("http://localhost:8081");
        DfsNode node2 = new DfsNode("http://localhost:8082");
        metaNodeService.registerNode(node1);
        metaNodeService.registerNode(node2);

        List<DfsNode> replicationNodes = metaNodeService.getReplicationNodes("testFile.txt", "http://localhost:8081");
        
        assertThat(replicationNodes).containsExactlyInAnyOrder(node1, node2);
    }

    @Test
    public void testGetRegisteredNodes() {
        DfsNode node1 = new DfsNode("http://localhost:8081");
        DfsNode node2 = new DfsNode("http://localhost:8082");
        metaNodeService.registerNode(node1);
        metaNodeService.registerNode(node2);
        
        List<DfsNode> registeredNodes = metaNodeService.getRegisteredNodes();
        
        assertThat(registeredNodes).containsExactlyInAnyOrder(node1, node2);
    }

    @Test
    public void testGetNodeFiles() {
        DfsNode node = new DfsNode("http://localhost:8081");
        metaNodeService.registerNode(node);
        metaNodeService.registerFileLocation("testFile.txt", node.getNodeUrl());

        List<String> files = metaNodeService.getNodeFiles(node.getNodeUrl());
        
        assertThat(files).containsExactly("testFile.txt");
    }

    @Test
    public void testClearCache() {
        DfsNode node = new DfsNode("http://localhost:8081");
        metaNodeService.registerNode(node);
        metaNodeService.registerFileLocation("testFile.txt", node.getNodeUrl());
        
        metaNodeService.clearCache();

        assertThat(metaNodeService.getNodeFiles(node.getNodeUrl())).isEmpty();
    }

    @Test
    public void testClearRegisteredNodes() {
        DfsNode node = new DfsNode("http://localhost:8081");
        metaNodeService.registerNode(node);
        
        metaNodeService.clearRegisteredNodes();

        assertThat(metaNodeService.getRegisteredNodes()).isEmpty();
    }

    @Test
    public void testSelectNodeForUpload() {
        DfsNode node1 = new DfsNode("http://localhost:8081");
        DfsNode node2 = new DfsNode("http://localhost:8082");
        DfsNode node3 = new DfsNode("http://localhost:8083");

        metaNodeService.registerNode(node1);
        metaNodeService.registerNode(node2);
        metaNodeService.registerNode(node3);

        // Track the nodes returned by successive calls.
        Set<DfsNode> selectedNodes = new HashSet<>();

        // Loop to call selectNodeForUpload multiple times and collect the results.
        for (int i = 0; i < 3; i++) {
            DfsNode selectedNode = metaNodeService.selectNodeForUpload();
            selectedNodes.add(selectedNode);
        }

        // Verify that each registered node is selected exactly once.
        assertThat(selectedNodes).containsExactlyInAnyOrder(node1, node2, node3);

        // Verify that after 3 selections, the next one restarts the round-robin sequence.
        DfsNode firstAgain = metaNodeService.selectNodeForUpload();
        assertThat(selectedNodes).contains(firstAgain);
    }
}
