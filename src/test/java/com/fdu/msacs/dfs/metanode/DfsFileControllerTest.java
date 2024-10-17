package com.fdu.msacs.dfs.metanode;

import com.fdu.msacs.dfs.metanode.meta.DfsFile;
import com.fdu.msacs.dfs.metanode.meta.FileMeta;
import com.fdu.msacs.dfs.metanode.meta.FileRefManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DfsFileControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private FileRefManager fileRefManager;

    private DfsFile testDfsFile;
    private String testHash;

    @BeforeEach
    public void setUp() {
        // Create a sample DfsFile object for testing
        testHash = "abc123456789defabcd5678abcdef12";
        FileMeta fileMeta = new FileMeta("path/to/file.txt", 12345, testHash);
        Set<String> blockHashes = Set.of("block1", "block2", "block3");
        testDfsFile = new DfsFile(fileMeta, blockHashes);
    }

    @Test
    public void testSaveDfsFile() {
        // Create an HTTP request with the DfsFile as the body
    	testDfsFile.getFileMeta().setHash(testHash);
        HttpEntity<DfsFile> request = new HttpEntity<>(testDfsFile);
        
        // Call the /metadata/save-dfs-file endpoint
        ResponseEntity<String> response = restTemplate.postForEntity("/metadata/save-dfs-file", request, String.class);

        // Validate the response
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("DfsFile with hash " + testHash + " has been saved successfully.", response.getBody());
    }

    @Test
    public void testGetDfsFile() throws IOException {
        // Save the testDfsFile first to make sure it exists in the repository
        fileRefManager.saveHashMapping(testHash, testDfsFile);

        // Call the /metadata/get-dfs-file endpoint with the test hash
        ResponseEntity<DfsFile> response = restTemplate.postForEntity(
                "/metadata/get-dfs-file?hash=" + testHash, null, DfsFile.class);

        // Validate the response
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        DfsFile retrievedFile = response.getBody();
        assertEquals(testDfsFile.getFileMeta(), retrievedFile.getFileMeta());
        assertEquals(testDfsFile.getBlockHashes(), retrievedFile.getBlockHashes());
    }

    @Test
    public void testGetDfsFileNotFound() {
        // Call the /metadata/get-dfs-file endpoint with a non-existing hash
        String nonExistingHash = "nonexistinghash123456";
        ResponseEntity<String> response = restTemplate.postForEntity(
                "/metadata/get-dfs-file?hash=" + nonExistingHash, null, String.class);

        // Validate the response
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("DfsFile with hash " + nonExistingHash + " not found.", response.getBody());
    }
}
