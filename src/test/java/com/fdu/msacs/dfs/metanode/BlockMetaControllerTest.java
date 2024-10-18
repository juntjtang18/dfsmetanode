package com.fdu.msacs.dfs.metanode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdu.msacs.dfs.metanode.BlockMetaController.RequestBlockNode;
import com.fdu.msacs.dfs.metanode.BlockMetaController.RequestNodesForBlock;
import com.fdu.msacs.dfs.metanode.BlockMetaController.RequestUnregisterBlock;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class BlockMetaControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private BlockMetaService blockMetaService; // Inject real service for testing

    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        objectMapper = new ObjectMapper();
        // You can initialize or reset your in-memory database here if needed
    }

    @Test
    public void testRegisterBlockLocation() {
        RequestBlockNode request = new RequestBlockNode();
        request.setHash("sampleHash");
        request.setNodeUrl("http://localhost:8080/node");

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/metadata/register-block-location",
                request,
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).startsWith("Block location registered"); // Adjust based on actual response
    }

    @Test
    public void testNodesForBlock() {
        RequestNodesForBlock request = new RequestNodesForBlock();
        request.setHash("sampleHash");
        request.setNodeUrl("http://localhost:8080/node");

        ResponseEntity<List<DfsNode>> response = restTemplate.postForEntity(
                "/metadata/nodes-for-block",
                request,
                (Class<List<DfsNode>>) (Object) List.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        // Further assertions can be added based on expected nodes
    }

    @Test
    public void testUnregisterBlock() {
        String hash = "sampleHash";

        ResponseEntity<String> response = restTemplate.exchange(
                "/metadata/unregister-block/{hash}",
                HttpMethod.DELETE,
                null,
                String.class,
                hash
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).startsWith("Block unregistered"); // Adjust based on actual response
    }

    @Test
    public void testUnregisterBlockFromNode() {
        RequestUnregisterBlock request = new RequestUnregisterBlock();
        request.setHash("sampleHash");
        request.setNodeUrl("http://localhost:8080/node");

        ResponseEntity<String> response = restTemplate.exchange(
                "/metadata/unregister-block-from-node",
                HttpMethod.DELETE,
                new HttpEntity<>(request),
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).startsWith("No block found for hash"); // Adjust based on actual response
    }

    // Additional tests can be added for error scenarios and edge cases.
}
