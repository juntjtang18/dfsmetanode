package com.infolink.dfs.metanode;


import com.infolink.dfs.shared.DfsFile;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class FileTreeControllerTest2 {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    private String baseUrl() {
        return "http://localhost:" + port + "/metadata";
    }

    @Test
    public void testCreateDirectory() {
        String url = baseUrl() + "/directory/create";
        Map<String, String> requestPayload = new HashMap<>();
        requestPayload.put("directory", "/new/directory/path");
        requestPayload.put("owner", "testOwner");

        ResponseEntity<String> response = restTemplate.postForEntity(url, requestPayload, String.class);

        // Verify response
        assertEquals(HttpStatus.OK, response.getStatusCode());
        //assertEquals("Directory created with hash: sampleDirectoryHash", response.getBody());
    }

    @Test
    public void testCreateSubdirectory() {
        String url = baseUrl() + "/directory/create-subdirectory";
        Map<String, String> requestPayload = new HashMap<>();
        requestPayload.put("directory", "subdir");
        requestPayload.put("parentDirectory", "/existing/path");
        requestPayload.put("owner", "testOwner");

        ResponseEntity<String> response = restTemplate.postForEntity(url, requestPayload, String.class);

        // Verify response
        assertEquals(HttpStatus.OK, response.getStatusCode());
        //assertEquals("Subdirectory created with final hash: sampleSubdirectoryHash", response.getBody());
    }
}
