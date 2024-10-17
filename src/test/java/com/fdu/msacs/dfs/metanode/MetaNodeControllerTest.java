package com.fdu.msacs.dfs.metanode;

import com.fdu.msacs.dfs.metanode.MetaNodeController.RequestUpload;
import com.fdu.msacs.dfs.metanode.MetaNodeController.UploadResponse;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT) // Use random port
public class MetaNodeControllerTest {

    @LocalServerPort
    private int port; // Random port assigned by Spring

    @Autowired
    private TestRestTemplate restTemplate; // TestRestTemplate for testing the endpoints

    private String baseUrl; // Base URL for the controller endpoints

    @BeforeEach
    public void setUp() {
        // Initialize the base URL for the endpoints with the random port
        baseUrl = "http://localhost:" + port + "/metadata"; // Use the injected port

        // Clear registered nodes and cache before each test
        clearRegisteredNodes();
        clearCache();
    }

    // Utility method to clear registered nodes
    private void clearRegisteredNodes() {
        ResponseEntity<String> clearNodesResponse = restTemplate.postForEntity(baseUrl + "/clear-registered-nodes", null, String.class);
        assertEquals(200, clearNodesResponse.getStatusCodeValue());
        assertEquals("Registered nodes cleared.", clearNodesResponse.getBody());
    }

    // Utility method to clear cache
    private void clearCache() {
        ResponseEntity<String> clearCacheResponse = restTemplate.postForEntity(baseUrl + "/clear-cache", null, String.class);
        assertEquals(200, clearCacheResponse.getStatusCodeValue());
        assertEquals("Cache cleared", clearCacheResponse.getBody());
    }

    // Utility method to register a node
    private void registerNode(DfsNode dfsNode) {
        ResponseEntity<String> response = restTemplate.postForEntity(baseUrl + "/register-node", dfsNode, String.class);
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Node registered: " + dfsNode.getContainerUrl(), response.getBody());
    }

    @Test
    public void testRegisterNode() {
        DfsNode dfsNode = new DfsNode("http://node1", "http://localhost/node1");
        registerNode(dfsNode); // Use the utility method
    }

    @Test
    public void testRegisterFileLocation() {
        DfsNode dfsNode = new DfsNode("http://node1", "http://localhost/node1");
        registerNode(dfsNode); // Register node first

        RequestFileLocation request = new RequestFileLocation("file.txt", "http://node1");
        ResponseEntity<String> response = restTemplate.postForEntity(baseUrl + "/register-file-location", request, String.class);
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("File location registered: file.txt on http://node1", response.getBody());
    }

    @Test
    public void testGetRegisteredNodes() {
        DfsNode dfsNode1 = new DfsNode("http://node1", "http://localhost/node1");
        DfsNode dfsNode2 = new DfsNode("http://node2", "http://localhost/node2");

        // Register nodes using the utility method
        registerNode(dfsNode1);
        registerNode(dfsNode2);

        ResponseEntity<List<DfsNode>> response = restTemplate.getForEntity(baseUrl + "/get-registered-nodes", (Class<List<DfsNode>>) (Class<?>) List.class);
        assertEquals(200, response.getStatusCodeValue());
        assertNotNull(response.getBody());
        assertEquals(2, response.getBody().size());
    }

    @Test
    public void testGetReplicationNodes() {
        RequestReplicationNodes request = new RequestReplicationNodes("file.txt", "http://node1");
        
        ResponseEntity<List<DfsNode>> response = restTemplate.postForEntity(baseUrl + "/get-replication-nodes", request, (Class<List<DfsNode>>) (Class<?>) List.class);
        assertEquals(200, response.getStatusCodeValue());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isEmpty()); // Assuming no nodes are registered yet
    }

    @Test
    public void testGetNodesForFile() {
        String filename = "file.txt";

        ResponseEntity<List<String>> response = restTemplate.getForEntity(baseUrl + "/nodes-for-file/" + filename, (Class<List<String>>) (Class<?>) List.class);
        assertEquals(200, response.getStatusCodeValue());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isEmpty()); // Assuming no file nodes are registered yet
    }

    @Test
    public void testGetFileNodeMapping() {
        String filename = "file.txt";

        ResponseEntity<List<String>> response = restTemplate.getForEntity(baseUrl + "/get-file-node-mapping/" + filename, (Class<List<String>>) (Class<?>) List.class);
        assertEquals(200, response.getStatusCodeValue());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isEmpty()); // Assuming no file-node mappings yet
    }

    @Test
    public void testClearRegisteredNodes() {
        DfsNode dfsNode = new DfsNode("http://node1", "http://localhost/node1");
        registerNode(dfsNode); // Register a node first

        ResponseEntity<String> response = restTemplate.postForEntity(baseUrl + "/clear-registered-nodes", null, String.class);
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Registered nodes cleared.", response.getBody());

        ResponseEntity<List<DfsNode>> nodesResponse = restTemplate.getForEntity(baseUrl + "/get-registered-nodes", (Class<List<DfsNode>>) (Class<?>) List.class);
        assertEquals(200, nodesResponse.getStatusCodeValue());
        assertEquals(0, nodesResponse.getBody().size());
    }

    @Test
    public void testPingSvr() {
        ResponseEntity<String> response = restTemplate.getForEntity(baseUrl + "/pingsvr", String.class);
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Metadata Server is running...", response.getBody());
    }

    @Test
    public void testGetUploadUrl() {
        // Create nodes with URLs for testing
        DfsNode dfsNode1 = new DfsNode("http://node1", "http://localhost:8081");
        DfsNode dfsNode2 = new DfsNode("http://node2", "http://localhost:8082");

        // Register nodes using a utility method (assuming this is available)
        registerNode(dfsNode1);
        registerNode(dfsNode2);

        // Create a RequestUpload object for testing
        RequestUpload requestUpload = new RequestUpload("some-unique-uuid", "example.txt");

        // Use restTemplate to send a POST request with the request body
        ResponseEntity<UploadResponse> response = restTemplate.postForEntity(
            baseUrl + "/upload-url",
            requestUpload,
            UploadResponse.class
        );

        // Validate the response
        assertEquals(200, response.getStatusCodeValue());
        UploadResponse responseBody = response.getBody();
        assertNotNull(responseBody);
        assertTrue(responseBody.getNodeUrl().startsWith("http://localhost"));
        assertNotNull(responseBody.getNodeUrl());
    }}
