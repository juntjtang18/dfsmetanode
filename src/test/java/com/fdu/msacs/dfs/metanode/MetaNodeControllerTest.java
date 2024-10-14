package com.fdu.msacs.dfs.metanode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdu.msacs.dfs.metanode.RequestFileLocation;
import com.fdu.msacs.dfs.metanode.RequestNode;
import com.fdu.msacs.dfs.metanode.RequestReplicationNodes;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class MetaNodeControllerTest {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeControllerTest.class);

    @LocalServerPort
    private int port;

    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        restTemplate = new RestTemplate();
        objectMapper = new ObjectMapper();
        // Clear the cache before each test
        clearCache();
        clearRegisteredNodes();
    }

    private String getBaseUrl() {
        return "http://localhost:" + port+ "/metadata";
    }

    @Test
    public void testRegisterNode() throws Exception {
        String nodeAddress = "http://localhost:8081/node1";
        String hostPort = "8081"; // You may want to set the port accordingly

        // Create an instance of DfsNode
        DfsNode dfsNode = new DfsNode(nodeAddress, hostPort);

        // Send POST request to register the node
        ResponseEntity<String> response = restTemplate.postForEntity(
                getBaseUrl() + "/register-node", 
                dfsNode, 
                String.class);
        
        // Assert the response
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("Node registered: " + nodeAddress);
    }

    @Test
    public void testRegisterFileLocation() throws Exception {
        RequestFileLocation request = new RequestFileLocation();
        request.setFilename("testfile.txt");
        request.setNodeUrl("http://localhost:8080/node1");

        ResponseEntity<String> response = restTemplate.postForEntity(
                getBaseUrl() + "/register-file-location",
                request,
                String.class
        );

        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("File location registered: testfile.txt on http://localhost:8080/node1");
    }

    @Test
    public void testGetNodesForFile() throws Exception {
        // First, register a node and file location
        String nodeAddress = "http://localhost:8080/node1";
        registerNode(nodeAddress, "8081");

        RequestFileLocation request = new RequestFileLocation();
        request.setFilename("testfile.txt");
        request.setNodeUrl(nodeAddress);
        restTemplate.postForEntity(getBaseUrl() + "/register-file-location", request, String.class);

        // Now, get nodes for the file
        ResponseEntity<List> response = restTemplate.getForEntity(
                getBaseUrl() + "/nodes-for-file/testfile.txt",
                List.class
        );

        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody().size()==1);
    }

    @Test
    public void testClearCache() throws Exception {
        // Add some nodes and file locations
        String nodeAddress = "http://localhost:8080/node1";
        RequestFileLocation request = new RequestFileLocation();
        request.setFilename("testfile.txt");
        request.setNodeUrl(nodeAddress);

        registerNode(nodeAddress, "80");
        restTemplate.postForEntity(getBaseUrl() + "/register-file-location", request, String.class);

        // Clear the cache
        ResponseEntity<String> response = restTemplate.postForEntity(getBaseUrl() + "/clear-cache", null, String.class);
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("Cache cleared");

        // Verify that the cache is indeed cleared
        ResponseEntity<List> nodesResponse = restTemplate.getForEntity(getBaseUrl() + "/nodes-for-file/testfile.txt", List.class);
        assertThat(nodesResponse.getStatusCodeValue()).isEqualTo(200);
        assertThat(nodesResponse.getBody()).isEmpty();  // Expect an empty list since the cache is cleared
    }

    @Test
    public void testGetRegisteredNodes() throws Exception {
        // Register some nodes
        String nodeAddress1 = "http://localhost:8080/node1";
        String nodeAddress2 = "http://localhost:8080/node2";
        registerNode(nodeAddress1, "8081");
        registerNode(nodeAddress2, "8082");
        
        // Now, get the registered nodes
        ResponseEntity<List<DfsNode>> response = restTemplate.exchange(
                getBaseUrl() + "/get-registered-nodes",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<DfsNode>>() {}
        );

        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).hasSize(2); // Assert that the response body contains exactly two elements

        // Assert that the response contains the expected node URLs
        assertThat(response.getBody()).extracting(DfsNode::getContainerUrl)
                .containsExactlyInAnyOrder(nodeAddress1, nodeAddress2);
    }


    @Test
    public void testGetFileNodeMapping() throws Exception {
        clearCache();
        clearRegisteredNodes();
        
        // Step 1: Register three nodes
        registerNode("http://localhost:8081", "8081");
        registerNode("http://localhost:8082", "8082");
        registerNode("http://localhost:8083", "8083");
        
        // Step 2: Register a file to nodes 2 and 3
        RequestFileLocation requestFileLocation1 = new RequestFileLocation();
        requestFileLocation1.setFilename("testFile.txt");
        requestFileLocation1.setNodeUrl("http://localhost:8082");
        restTemplate.postForEntity(getBaseUrl() + "/register-file-location", requestFileLocation1, String.class);
        
        RequestFileLocation requestFileLocation2 = new RequestFileLocation();
        requestFileLocation2.setFilename("testFile.txt");
        requestFileLocation2.setNodeUrl("http://localhost:8083");
        restTemplate.postForEntity(getBaseUrl() + "/register-file-location", requestFileLocation2, String.class);

        // Step 3: Call the endpoint and verify the response
        ResponseEntity<List<DfsNode>> response = restTemplate.exchange(
            getBaseUrl() + "/get-file-node-mapping/testFile.txt",
            HttpMethod.GET,
            null,
            new ParameterizedTypeReference<List<DfsNode>>() {}
        );

        // Step 4: Assert the expected outcome
        assertEquals(200, response.getStatusCodeValue());

        // Step 5: Assert that the response contains the expected DfsNode URLs
        List<DfsNode> dfsNodes = response.getBody();
        assertNotNull(dfsNodes);
        assertThat(dfsNodes).hasSize(2); // Ensure there are 2 nodes

        // Verify that the list contains nodes with the expected URLs
        assertThat(dfsNodes).extracting(DfsNode::getContainerUrl)
            .containsExactlyInAnyOrder("http://localhost:8082", "http://localhost:8083");
    }




    public void registerNode(String nodeAddress, String hostPort) {
        // Create an instance of DfsNode
        DfsNode dfsNode = new DfsNode(nodeAddress, hostPort);

        // Send POST request to register the node
        ResponseEntity<String> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/metadata/register-node", 
                dfsNode, 
                String.class);
        
        // Assert the response
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("Node registered: " + nodeAddress);
    }

    @Test
    void clearCache() {
        ResponseEntity<String> response = restTemplate.postForEntity("http://localhost:" + port + "/metadata/clear-cache", null, String.class);
        
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Cache cleared", response.getBody());
    }

    @Test
    void clearRegisteredNodes() {
    	ResponseEntity<String> response = restTemplate.postForEntity("http://localhost:" + port + "/metadata/clear-registered-nodes", null, String.class);        
        
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Registered nodes cleared.", response.getBody());
    }

    @Test
    void pingSvr() {
        ResponseEntity<String> response = restTemplate.exchange(getBaseUrl() + "/pingsvr", HttpMethod.GET, null, String.class);
        
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Metadata Server is running...", response.getBody());
    }
    
    @Test
    void testUploadUrl() {
        // Step 1: Clear cache and registered nodes
    	clearCache();
    	clearRegisteredNodes();
    	
        // Step 2: Register three nodes
    	String node1 = "http://node1:8081";
    	String node2 = "http://node2:8081";
    	String node3 = "http://node3:8081";
    	
        registerNode(node1, "8081");
        registerNode(node2, "8082");
        registerNode(node3, "8083");
        
        ResponseEntity<String> response1 = restTemplate.exchange(getBaseUrl() + "/upload-url", HttpMethod.GET, null, String.class);
        assertEquals(200, response1.getStatusCodeValue());
        
        ResponseEntity<String> response2 = restTemplate.exchange(getBaseUrl() + "/upload-url", HttpMethod.GET, null, String.class);
        assertEquals(200, response2.getStatusCodeValue());
        
        ResponseEntity<String> response3 = restTemplate.exchange(getBaseUrl() + "/upload-url", HttpMethod.GET, null, String.class);
        assertEquals(200, response3.getStatusCodeValue());
        
        ResponseEntity<String> response4 = restTemplate.exchange(getBaseUrl() + "/upload-url", HttpMethod.GET, null, String.class);
        assertEquals(200, response4.getStatusCodeValue());
        
    }
}
