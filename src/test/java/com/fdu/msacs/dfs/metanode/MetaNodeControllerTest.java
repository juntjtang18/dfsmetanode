package com.fdu.msacs.dfs.metanode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdu.msacs.dfs.metanode.RequestFileLocation;
import com.fdu.msacs.dfs.metanode.RequestNode;
import com.fdu.msacs.dfs.metanode.RequestReplicationNodes;

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
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/clear-cache", null, String.class);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/clear-registered-nodes", null, String.class);        
    }

    private String getBaseUrl() {
        return "http://localhost:" + port+ "/metadata";
    }

    @Test
    public void testRegisterNode() throws Exception {
        String nodeAddress = "http://localhost:8081/node1";
        RequestNode request = new RequestNode();
        request.setNodeUrl(nodeAddress);
        ResponseEntity<String> response = restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-node", request, String.class);
        
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("Node registered: " + nodeAddress);
    }

    @Test
    public void testRegisterFileLocation() throws Exception {
        RequestFileLocation request = new RequestFileLocation();
        request.setFilename("testfile.txt");
        request.setNodeUrl("http://localhost:8080/node1");

        ResponseEntity<String> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/metadata/register-file-location",
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
        registerNode(nodeAddress);

        RequestFileLocation request = new RequestFileLocation();
        request.setFilename("testfile.txt");
        request.setNodeUrl(nodeAddress);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-file-location", request, String.class);

        // Now, get nodes for the file
        ResponseEntity<List> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/metadata/nodes-for-file/testfile.txt",
                List.class
        );

        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).containsExactly(nodeAddress);
    }

    @Test
    public void testClearCache() throws Exception {
        // Add some nodes and file locations
        String nodeAddress = "http://localhost:8080/node1";
        RequestFileLocation request = new RequestFileLocation();
        request.setFilename("testfile.txt");
        request.setNodeUrl(nodeAddress);

        registerNode(nodeAddress);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-file-location", request, String.class);

        // Clear the cache
        ResponseEntity<String> response = restTemplate.postForEntity("http://localhost:" + port + "/metadata/clear-cache", null, String.class);
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("Cache cleared");

        // Verify that the cache is indeed cleared
        ResponseEntity<List> nodesResponse = restTemplate.getForEntity("http://localhost:" + port + "/metadata/nodes-for-file/testfile.txt", List.class);
        assertThat(nodesResponse.getStatusCodeValue()).isEqualTo(200);
        assertThat(nodesResponse.getBody()).isEmpty();  // Expect an empty list since the cache is cleared
    }

    @Test
    public void testGetRegisteredNodes() throws Exception {
        // Register some nodes
        String nodeAddress1 = "http://localhost:8080/node1";
        String nodeAddress2 = "http://localhost:8080/node2";
        registerNode(nodeAddress1);
        registerNode(nodeAddress2);
        
        // Now, get the registered nodes
        ResponseEntity<List> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/metadata/get-registered-nodes",
                List.class
        );

        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).containsExactlyInAnyOrder(nodeAddress1, nodeAddress2);
    }

    @Test
    public void testGetFileNodeMapping() throws Exception {
    	clearCache();
    	clearRegisteredNodes();
    	
        // Step 1: Register three nodes
    	registerNode("http://localhost:8081");
    	registerNode("http://localhost:8082");
    	registerNode("http://localhost:8083");
    	
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
        ResponseEntity<List<String>> response = restTemplate.exchange(
            getBaseUrl() + "/get-file-node-mapping/testFile.txt",
            HttpMethod.GET,
            null,
            new ParameterizedTypeReference<List<String>>() {}
        );

        // Step 4: Assert the expected outcome
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(List.of("http://localhost:8082", "http://localhost:8083"), response.getBody());
    }


    @Test
    public void testGetReplicationNodes() throws Exception {
        // Register nodes with special characters
        String nodeAddress1 = "http://node1.com:8080";
        String nodeAddress2 = "http://node2.com:8080";
        String nodeAddress3 = "http://node3.com:8080";
        String nodeAddress4 = "http://node4.com:8080";

        // Register the nodes
        registerNode(nodeAddress1);
        registerNode(nodeAddress2);
        registerNode(nodeAddress3);
        registerNode(nodeAddress4);
        
        // Prepare request object
        RequestReplicationNodes replicationRequest = new RequestReplicationNodes();
        replicationRequest.setFilename("testfile.txt");
        replicationRequest.setRequestingNodeUrl(nodeAddress1);

        // Case 1: Node 1 requests replication nodes, and none have the file
        ResponseEntity<List> response1 = restTemplate.postForEntity(
                "http://localhost:" + port + "/metadata/get-replication-nodes", 
                replicationRequest, 
                List.class
        );

        // Expect to get nodeAddress2, nodeAddress3, and nodeAddress4
        assertThat(response1.getStatusCodeValue()).isEqualTo(200);
        assertThat(response1.getBody()).containsExactlyInAnyOrder(nodeAddress2, nodeAddress3, nodeAddress4);

        // Case 2: Register file location only on Node 2
        RequestFileLocation requestFileNode2 = new RequestFileLocation();
        requestFileNode2.setFilename("testfile.txt");
        requestFileNode2.setNodeUrl(nodeAddress2);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-file-location", requestFileNode2, String.class);

        // Node 1 requests replication nodes again, now Node 2 has the file
        ResponseEntity<List> response2 = restTemplate.postForEntity(
                "http://localhost:" + port + "/metadata/get-replication-nodes", 
                replicationRequest, 
                List.class
        );

        // Expect to get nodeAddress3 and nodeAddress4
        assertThat(response2.getStatusCodeValue()).isEqualTo(200);
        assertThat(response2.getBody()).containsExactlyInAnyOrder(nodeAddress3, nodeAddress4);

        // Case 3: Register file location on all other nodes
        RequestFileLocation requestFileNode3 = new RequestFileLocation();
        requestFileNode3.setFilename("testfile.txt");
        requestFileNode3.setNodeUrl(nodeAddress3);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-file-location", requestFileNode3, String.class);

        RequestFileLocation requestFileNode4 = new RequestFileLocation();
        requestFileNode4.setFilename("testfile.txt");
        requestFileNode4.setNodeUrl(nodeAddress4);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-file-location", requestFileNode4, String.class);

        // Node 1 requests replication nodes again, now all other nodes have the file
        ResponseEntity<List> response3 = restTemplate.postForEntity(
                "http://localhost:" + port + "/metadata/get-replication-nodes", 
                replicationRequest, 
                List.class
        );

        // Expect to get an empty list
        assertThat(response3.getStatusCodeValue()).isEqualTo(200);
        assertThat(response3.getBody()).isEmpty();
    }
    @Test
    public void testGetNodeFiles() throws Exception {
        // Register a node
        String nodeAddress = "http://localhost:8080/node1";
        registerNode(nodeAddress);

        // Register some files for the node
        RequestFileLocation requestFile1 = new RequestFileLocation();
        requestFile1.setFilename("file1.txt");
        requestFile1.setNodeUrl(nodeAddress);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-file-location", requestFile1, String.class);

        RequestFileLocation requestFile2 = new RequestFileLocation();
        requestFile2.setFilename("file2.txt");
        requestFile2.setNodeUrl(nodeAddress);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-file-location", requestFile2, String.class);

        // Prepare the request for retrieving node files
        RequestNode requestNode = new RequestNode();
        requestNode.setNodeUrl(nodeAddress);

        // Now, get the files registered to the node
        ResponseEntity<List> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/metadata/get-node-files",
                requestNode,
                List.class
        );

        // Assert the response
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).containsExactlyInAnyOrder("file1.txt", "file2.txt");
    }
    
    public void registerNode(String nodeAddress) {
        RequestNode request = new RequestNode();
        request.setNodeUrl(nodeAddress);
        
        ResponseEntity<String> response = restTemplate.postForEntity(
        		"http://localhost:" + port + "/metadata/register-node", 
        		request, 
        		String.class);
        
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
    void registerFileLocation() {
        RequestFileLocation request = new RequestFileLocation();
        request.setFilename("testFile.txt");
        request.setNodeUrl("http://localhost:8081");

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        HttpEntity<RequestFileLocation> entity = new HttpEntity<>(request, headers);

        ResponseEntity<String> response = restTemplate.exchange(getBaseUrl() + "/register-file-location", HttpMethod.POST, entity, String.class);
        
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("File location registered: testFile.txt on http://localhost:8081", response.getBody());
    }

    @Test
    void getNodesForFile() {
    	clearCache();
    	clearRegisteredNodes();
    	
        // Step 1: Register multiple nodes
        String node1 = "http://localhost:8081";
        String node2 = "http://localhost:8082";
        String node3 = "http://localhost:8083";
        

        registerNode(node1);
        registerNode(node2);
        registerNode(node3);
        
        // Step 2: Register a file location to node1
        RequestFileLocation fileRequest = new RequestFileLocation();
        fileRequest.setFilename("testFile.txt");
        fileRequest.setNodeUrl(node1);
        
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        HttpEntity<RequestFileLocation> fileRequestEntity = new HttpEntity<>(fileRequest, headers);
        restTemplate.exchange(getBaseUrl() + "/register-file-location", HttpMethod.POST, fileRequestEntity, String.class);

        // Step 3: Call the endpoint to get nodes for the file
        ResponseEntity<List<String>> response = restTemplate.exchange(getBaseUrl() + "/nodes-for-file/testFile.txt", HttpMethod.GET, null, new ParameterizedTypeReference<List<String>>() {});

        // Assert the response status code
        assertEquals(200, response.getStatusCodeValue());

        // Assert that the response contains the expected nodes (node2 and node3)
        List<String> expectedNodes = List.of(node1);
        assertEquals(expectedNodes, response.getBody());
    }


    @Test
    void getReplicationNodes() {
        clearCache();
        clearRegisteredNodes();
        
        // Step 1: Register three nodes
        String node1 = "http://localhost:8081";
        String node2 = "http://localhost:8082";
        String node3 = "http://localhost:8083";
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Registering node1
        RequestNode requestNode1 = new RequestNode();
        requestNode1.setNodeUrl(node1);
        HttpEntity<RequestNode> requestEntity1 = new HttpEntity<>(requestNode1, headers);
        restTemplate.exchange(getBaseUrl() + "/register-node", HttpMethod.POST, requestEntity1, String.class);

        // Registering node2
        RequestNode requestNode2 = new RequestNode();
        requestNode2.setNodeUrl(node2);
        HttpEntity<RequestNode> requestEntity2 = new HttpEntity<>(requestNode2, headers);
        restTemplate.exchange(getBaseUrl() + "/register-node", HttpMethod.POST, requestEntity2, String.class);

        // Registering node3
        RequestNode requestNode3 = new RequestNode();
        requestNode3.setNodeUrl(node3);
        HttpEntity<RequestNode> requestEntity3 = new HttpEntity<>(requestNode3, headers);
        restTemplate.exchange(getBaseUrl() + "/register-node", HttpMethod.POST, requestEntity3, String.class);

        // Step 2: Register a file location to node2
        RequestFileLocation fileRequest = new RequestFileLocation();
        fileRequest.setFilename("testFile.txt");
        fileRequest.setNodeUrl(node2);

        HttpEntity<RequestFileLocation> fileRequestEntity = new HttpEntity<>(fileRequest, headers);
        restTemplate.exchange(getBaseUrl() + "/register-file-location", HttpMethod.POST, fileRequestEntity, String.class);

        // Step 3: Create the request for replication nodes
        RequestReplicationNodes request = new RequestReplicationNodes();
        request.setFilename("testFile.txt");
        request.setRequestingNodeUrl(node2);

        HttpEntity<RequestReplicationNodes> entity = new HttpEntity<>(request, headers);

        // Step 4: Call the endpoint to get replication nodes
        ResponseEntity<List<String>> response = restTemplate.exchange(
            getBaseUrl() + "/get-replication-nodes",
            HttpMethod.POST,
            entity,
            new ParameterizedTypeReference<List<String>>() {}
        );

        // Assert the response status code
        assertEquals(200, response.getStatusCodeValue());

        // Sort both the expected nodes and the response body before comparing
        List<String> expectedNodes = List.of(node1, node3);
        List<String> sortedExpectedNodes = new ArrayList<>(expectedNodes);
        List<String> sortedResponseNodes = new ArrayList<>(response.getBody());

        Collections.sort(sortedExpectedNodes);
        Collections.sort(sortedResponseNodes);

        // Assert that the sorted lists are equal
        assertEquals(sortedExpectedNodes, sortedResponseNodes);
    }



    @Test
    void getRegisteredNodes() {
        ResponseEntity<List<String>> response = restTemplate.exchange(getBaseUrl() + "/get-registered-nodes", HttpMethod.GET, null, new ParameterizedTypeReference<List<String>>() {});
        
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(List.of(), response.getBody()); // Adjust based on expected outcome
    }

    @Test
    void getNodeFiles() {
        // Step 1: Clear cache and registered nodes
    	clearCache();
    	clearRegisteredNodes();

        // Step 2: Register three nodes
        registerNode("http://localhost:8081");
        registerNode("http://localhost:8082");
        registerNode("http://localhost:8083");
        
        // Step 3: Register three files to node 1
        RequestFileLocation requestFileLocation1 = new RequestFileLocation();
        requestFileLocation1.setFilename("file1.txt");
        requestFileLocation1.setNodeUrl("http://localhost:8081");
        restTemplate.postForEntity(getBaseUrl() + "/register-file-location", requestFileLocation1, String.class);

        RequestFileLocation requestFileLocation2 = new RequestFileLocation();
        requestFileLocation2.setFilename("file2.txt");
        requestFileLocation2.setNodeUrl("http://localhost:8081");
        restTemplate.postForEntity(getBaseUrl() + "/register-file-location", requestFileLocation2, String.class);

        RequestFileLocation requestFileLocation3 = new RequestFileLocation();
        requestFileLocation3.setFilename("file3.txt");
        requestFileLocation3.setNodeUrl("http://localhost:8081");
        restTemplate.postForEntity(getBaseUrl() + "/register-file-location", requestFileLocation3, String.class);

        // Step 4: Call the endpoint and verify the response
        RequestNode request = new RequestNode();
        request.setNodeUrl("http://localhost:8081");

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        HttpEntity<RequestNode> entity = new HttpEntity<>(request, headers);

        ResponseEntity<List<String>> response = restTemplate.exchange(
            getBaseUrl() + "/get-node-files",
            HttpMethod.POST,
            entity,
            new ParameterizedTypeReference<List<String>>() {}
        );

        // Step 5: Assert the expected outcome
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(List.of("file1.txt", "file2.txt", "file3.txt"), response.getBody());
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
    	
        registerNode(node1);
        registerNode(node2);
        registerNode(node3);
        
        ResponseEntity<String> response1 = restTemplate.exchange(getBaseUrl() + "/upload-url", HttpMethod.GET, null, String.class);
        assertEquals(200, response1.getStatusCodeValue());
        assertEquals(node2 + "/upload", response1.getBody());
        
        ResponseEntity<String> response2 = restTemplate.exchange(getBaseUrl() + "/upload-url", HttpMethod.GET, null, String.class);
        assertEquals(200, response2.getStatusCodeValue());
        assertEquals(node1 + "/upload", response2.getBody());
        
        ResponseEntity<String> response3 = restTemplate.exchange(getBaseUrl() + "/upload-url", HttpMethod.GET, null, String.class);
        assertEquals(200, response3.getStatusCodeValue());
        assertEquals(node3 + "/upload", response3.getBody());
        
        ResponseEntity<String> response4 = restTemplate.exchange(getBaseUrl() + "/upload-url", HttpMethod.GET, null, String.class);
        assertEquals(200, response4.getStatusCodeValue());
        assertEquals(node2 + "/upload", response4.getBody());
        
    }
}
