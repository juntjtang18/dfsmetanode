package com.fdu.msacs.dfs.metanode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdu.msacs.dfs.metanode.BlockMetaController.RequestBlockNode;
import com.fdu.msacs.dfs.metanode.BlockMetaController.RequestNodesForBlock;
import com.fdu.msacs.dfs.metanode.BlockMetaController.RequestUnregisterBlock;
import com.fdu.msacs.dfs.metanode.mdb.BlockNode;
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
        // Create a request object for registering the block location
        RequestBlockNode request = new RequestBlockNode();
        request.setHash("sampleHash");
        request.setNodeUrl("http://localhost:8080/node");

        // Make the POST request to register the block location
        ResponseEntity<String> response = restTemplate.postForEntity(
                "/metadata/register-block-location",
                request,
                String.class
        );

        // Verify the response status and body
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).startsWith("Block location registered");

        // Use BlockMetaService::getBlockNodeByHash to verify the registered node
        BlockNode blockNode = blockMetaService.getBlockNodeByHash("sampleHash");

        // Ensure that the blockNode is not null and contains the registered node URL
        //assertNotNull(blockNode, "BlockNode should not be null");
        assertThat(blockNode.getNodeUrls()).contains("http://localhost:8080/node");
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
        //assertThat(response.getBody()).startsWith("No block found"); // Adjust based on actual response
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

    @Test
    public void testClearAllBlockNodes() {
        // Initially, we can register a few block nodes to set up the scenario.
        RequestBlockNode request1 = new RequestBlockNode();
        request1.setHash("sampleHash1");
        request1.setNodeUrl("http://localhost:8080/node1");
        restTemplate.postForEntity("/metadata/register-block-location", request1, String.class);

        RequestBlockNode request2 = new RequestBlockNode();
        request2.setHash("sampleHash2");
        request2.setNodeUrl("http://localhost:8080/node2");
        restTemplate.postForEntity("/metadata/register-block-location", request2, String.class);

        // Now clear all block nodes
        ResponseEntity<String> response = restTemplate.exchange(
                "/metadata/clear-all-block-nodes-mapping",
                HttpMethod.DELETE,
                null,
                String.class
        );

        // Verify the response
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("All block nodes have been cleared.");

        // Verify that no block nodes exist after clearing
        assertThat(blockMetaService.getBlockNodeByHash("sampleHash1")).isNull();
        assertThat(blockMetaService.getBlockNodeByHash("sampleHash2")).isNull();
    }
}
