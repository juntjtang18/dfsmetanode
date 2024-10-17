package com.fdu.msacs.dfs.metanode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fdu.msacs.dfs.metanode.BlockMetaController.RequestBlockNode;
import com.fdu.msacs.dfs.metanode.BlockMetaController.RequestUnregisterBlock;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class BlockControllerIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;
    @LocalServerPort
    private int port;
    
    private final String baseUrl = "/metadata";

    @BeforeEach
    void setUp() {
        // Optionally, you can set up test data here
    }

    @Test
    public void testRegisterBlockLocation() {
    	RequestBlockNode request = new RequestBlockNode();
        request.setHash("sampleHash");
        request.setNodeUrl("http://localhost:8080/node1");

        ResponseEntity<String> response = restTemplate.postForEntity(
                baseUrl + "/register-block-location", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("Block location registered");
    }

    @Test
    public void testBlockExists() {
        // Assuming that you have registered a block with this hash first
    	testRegisterBlockLocation();
        String hash = "sampleHash";
        ResponseEntity<Set<String>> response = restTemplate.exchange(
                baseUrl + "/block-exists/" + hash,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<Set<String>>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        // Optionally, assert that the returned set is not empty
        assertThat(response.getBody()).isNotEmpty();
    }


    @Test
    public void testUnregisterBlock() {
    	testRegisterBlockLocation();
    	
        String hash = "sampleHash";

        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl + "/unregister-block/" + hash, 
                HttpMethod.DELETE, null, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("Block unregistered: ");
    }

    @Test
    public void testUnregisterBlockFromNode() {
        RequestUnregisterBlock request = new RequestUnregisterBlock();
        request.setHash("sampleHash");
        request.setNodeUrl("http://localhost:8080/node1");

        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl + "/unregister-block-from-node", 
                HttpMethod.DELETE, new HttpEntity<>(request), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("Block fully unregistered as no node URLs remain: ");
    }
}
