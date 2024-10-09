package com.fdu.msacs.dfsmetasvr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.assertThat;

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
    }

    @Test
    public void testRegisterNode() throws Exception {
        String nodeAddress = "http://localhost:8080/node1";

        ResponseEntity<String> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/metadata/register-node",
                nodeAddress,
                String.class
        );

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
        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-node", nodeAddress, String.class);

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

        restTemplate.postForEntity("http://localhost:" + port + "/metadata/register-node", nodeAddress, String.class);
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

}
