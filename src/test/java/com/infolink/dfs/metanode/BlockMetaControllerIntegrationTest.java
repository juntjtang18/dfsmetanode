package com.infolink.dfs.metanode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.ActiveProfiles;

import com.infolink.dfs.shared.DfsNode;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test") // Use a separate profile for testing if needed
public class BlockMetaControllerIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private BlockMetaService blockMetaService;

    private String baseUrl;

    @BeforeEach
    public void setUp() {
        baseUrl = "http://localhost:" + port;
    }

    @Test
    public void testRegisterBlockLocation() {
        BlockMetaController.RequestBlockNode request = new BlockMetaController.RequestBlockNode();
        request.setHash("testHash");
        request.setNodeUrl("http://node1.example.com");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<BlockMetaController.RequestBlockNode> entity = new HttpEntity<>(request, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(baseUrl + "/metadata/block/register-block-location", entity, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(true, response.getBody().startsWith("Block location registered:"));
    }

    @Test
    public void testNodesForBlock() {
        String hash = "testHash";
        String requestingNodeUrl = "http://requestingNode.example.com";

        // Register 5 nodes using the provided endpoint
        registerNode("http://node1.example.com");
        registerNode("http://node2.example.com");
        registerNode("http://node3.example.com");
        registerNode("http://node4.example.com");
        registerNode("http://node5.example.com");

        // Prepare the request object
        BlockMetaController.RequestNodesForBlock request = new BlockMetaController.RequestNodesForBlock();
        request.setHash(hash);
        request.setNodeUrl(requestingNodeUrl);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<BlockMetaController.RequestNodesForBlock> entity = new HttpEntity<>(request, headers);

        // Call the nodes-for-block endpoint
        ResponseEntity<BlockMetaController.ResponseNodesForBlock> response = restTemplate.postForEntity(
                baseUrl + "/metadata/block/nodes-for-block", entity, BlockMetaController.ResponseNodesForBlock.class);

        // Assert that the response is OK and contains the expected data
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(BlockMetaController.ResponseNodesForBlock.Status.SUCCESS, response.getBody().getStatus());

        // Verify that the response contains the list of nodes and has 5 registered nodes
        List<DfsNode> nodes = response.getBody().getNodes();
        assertNotNull(nodes);
        assertEquals(3, nodes.size());
    }

    private void registerNode(String nodeUrl) {
        DfsNode node = new DfsNode();
        node.setContainerUrl(nodeUrl);
        node.setLocalUrl(nodeUrl);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<DfsNode> entity = new HttpEntity<>(node, headers);

        // Call the register-node endpoint to register the node
        ResponseEntity<String> response = restTemplate.postForEntity(
                baseUrl + "/metadata/register-node", entity, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(true, response.getBody().startsWith("Node registered:"));
    }

    @Test
    public void testBlockNodes() {
        String hash = "testHash";

        // Assume the block is registered before testing this endpoint
        blockMetaService.registerBlockLocation(hash, "http://node1.example.com");

        ResponseEntity<List> response = restTemplate.getForEntity(baseUrl + "/metadata/block/block-nodes/" + hash, List.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().size());
        assertEquals("http://node1.example.com", response.getBody().get(0));
    }

    @Test
    public void testUnregisterBlock() {
        String hash = "testHash";

        // Assume the block is registered before testing this endpoint
        blockMetaService.registerBlockLocation(hash, "http://node1.example.com");

        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl + "/metadata/block/unregister-block/" + hash,
                HttpMethod.DELETE,
                null,
                String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(response.getBody().startsWith("Block unregistered:"), true);
    }

    @Test
    public void testUnregisterBlockFromNode() {
        BlockMetaController.RequestUnregisterBlock request = new BlockMetaController.RequestUnregisterBlock();
        request.setHash("testHash");
        request.setNodeUrl("http://node1.example.com");

        // Assume the block is registered before testing this endpoint
        blockMetaService.registerBlockLocation("testHash", "http://node1.example.com");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<BlockMetaController.RequestUnregisterBlock> entity = new HttpEntity<>(request, headers);
        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl + "/metadata/block/unregister-block-from-node",
                HttpMethod.DELETE,
                entity,
                String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(true, response.getBody().startsWith("Block fully unregistered as no node URLs remain:"));
    }

    @Test
    public void testClearAllBlocks() {
        // Register some block locations before clearing
        blockMetaService.registerBlockLocation("hash1", "http://node1.example.com");
        blockMetaService.registerBlockLocation("hash2", "http://node2.example.com");

        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl + "/metadata/block/clear-all-block-nodes-mapping",
                HttpMethod.DELETE,
                null,
                String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("All block nodes have been cleared.", response.getBody());
    }
}
