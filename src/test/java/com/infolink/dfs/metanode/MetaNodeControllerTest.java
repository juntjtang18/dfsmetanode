package com.infolink.dfs.metanode;

import com.infolink.dfs.shared.DfsNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class MetaNodeControllerTest {

    @LocalServerPort
    private int port;

    private String baseUrl;

    @Autowired
    private TestRestTemplate restTemplate;

    @BeforeEach
    public void setUp() {
        baseUrl = "http://localhost:" + port + "/metadata";
        testClearRegisteredNodes();
    }

    @Test
    public void testRegisterNode() {
        DfsNode dfsNode = new DfsNode("http://localhost:8081", "http://localhost:8081");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<DfsNode> request = new HttpEntity<>(dfsNode, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(baseUrl + "/register-node", request, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().startsWith("Node registered:"));
    }

    @Test
    public void testGetRegisteredNodes() {
        ResponseEntity<List> response = restTemplate.getForEntity(baseUrl + "/get-registered-nodes", List.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @Test
    public void testClearRegisteredNodes() {
        ResponseEntity<String> response = restTemplate.postForEntity(baseUrl + "/clear-registered-nodes", null, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Registered nodes cleared.", response.getBody());
    }
    
    void registerNode(DfsNode node) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<DfsNode> request = new HttpEntity<>(node, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(baseUrl + "/register-node", request, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
    	
    }
    
    @Test
    public void testGetUploadUrl() {
    	DfsNode node1 = new DfsNode("node1", "node1");
    	DfsNode node2 = new DfsNode("node2", "node2");
    	DfsNode node3 = new DfsNode("node3", "node3");
    	registerNode(node1);
    	registerNode(node2);
    	registerNode(node3);
    	
        MetaNodeController.RequestUpload requestUpload = new MetaNodeController.RequestUpload(
            "uuid-1234", "testfile.txt", "/uploads", "owner1"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<MetaNodeController.RequestUpload> request = new HttpEntity<>(requestUpload, headers);

        ResponseEntity<MetaNodeController.UploadResponse> response = restTemplate.postForEntity(
            baseUrl + "/upload-url", request, MetaNodeController.UploadResponse.class
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertNotNull(response.getBody().getNodeUrl());
    }

    @Test
    public void testPingSvr() {
        ResponseEntity<String> response = restTemplate.getForEntity(baseUrl + "/pingsvr", String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Metadata Server is running...", response.getBody());
    }
}
