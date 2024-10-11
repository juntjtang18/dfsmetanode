package com.fdu.msacs.dfsmetanode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdu.msacs.dfsmetanode.RequestFileLocation;
import com.fdu.msacs.dfsmetanode.RequestNode;
import com.fdu.msacs.dfsmetanode.RequestReplicationNodes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class MetadataControllerTest {

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
        assertThat(nodesResponse.getBody()).isNull();  // Expect null or empty since the cache is cleared
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
        // Register a node
        String nodeAddress = "http://localhost:8080/node1";
        registerNode(nodeAddress);

        // Register file location on the node
        RequestFileLocation request = new RequestFileLocation();
        request.setFilename("testfile.txt");
        request.setNodeUrl(nodeAddress);
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-file-location", request, String.class);

        // Now, get the file node mapping for "testfile.txt"
        ResponseEntity<List> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/metadata/get-file-node-mapping/testfile.txt",
                List.class
        );

        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).containsExactly(nodeAddress);
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
}
