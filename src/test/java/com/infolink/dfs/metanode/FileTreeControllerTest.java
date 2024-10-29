package com.infolink.dfs.metanode;

import com.infolink.dfs.metanode.mdb.BlockNode;
import com.infolink.dfs.shared.DfsFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;

import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class FileTreeControllerTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private FileTreeManager fileTreeManager;
    
    @Autowired
    private BlockMetaService blockMetaService;
    
    private String baseUrl;

    @BeforeEach
    public void setup() {
        fileTreeManager.clearAllData();
        baseUrl = "http://localhost:" + port;
    }

    private ResponseEntity<String> saveTestFile(DfsFile dfsFile, String targetDirectory) {
        // Create the request object
        FileTreeController.RequestSaveFile request = new FileTreeController.RequestSaveFile();
        request.setDfsFile(dfsFile);
        request.setTargetDirectory(targetDirectory);

        // Create the headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<FileTreeController.RequestSaveFile> requestEntity = new HttpEntity<>(request, headers);

        // Send the POST request to save the file
        ResponseEntity<String> response = restTemplate.postForEntity(
                baseUrl + "/metadata/file/save", requestEntity, String.class);

        // Verify that the file is saved successfully
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return response;
    }

    @Test
    public void testSaveFile() {
        // Create a DfsFile object to send in the request body
        DfsFile dfsFile = new DfsFile();
        dfsFile.setHash("hash1234");
        dfsFile.setName("example2.txt");
        dfsFile.setOwner("testOwner");
        dfsFile.setPath("/test-directory/example.txt");
        dfsFile.setSize(1024);
        dfsFile.setDirectory(false);
        dfsFile.setParentHash("parentHash123");
        dfsFile.setBlockHashes(Collections.singletonList("blockHash1"));
        dfsFile.setCreateTime(new Date());
        dfsFile.setLastModifiedTime(new Date());

        // Define the target directory
        String targetDirectory = "/test-directory";

        // Save the test file
        //saveTestFile(dfsFile, targetDirectory);

        // Verify the response status and body
        ResponseEntity<String> response = saveTestFile(dfsFile, targetDirectory);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isEqualTo("File saved successfully.");
    }

    @Test
    public void testGetFile() {
        // Initialize a test file
        DfsFile testFile = new DfsFile();
        testFile.setHash("hash123");
        testFile.setName("example.txt");
        testFile.setOwner("testOwner");
        testFile.setPath("/test-directory/example.txt");
        testFile.setSize(1024);
        testFile.setDirectory(false);
        testFile.setParentHash("parentHash123");
        testFile.setBlockHashes(Collections.singletonList("blockHash1"));
        testFile.setCreateTime(new Date());
        testFile.setLastModifiedTime(new Date());

        // Save the test file before each test to ensure consistency
        saveTestFile(testFile, "/test-directory");
        String fileHash = "hash123";

        ResponseEntity<DfsFile> response = restTemplate.getForEntity(
                baseUrl + "/metadata/file/" + fileHash, DfsFile.class);

        // Verify the response status and the retrieved file
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        DfsFile retrievedFile = response.getBody();
        assertThat(retrievedFile).isNotNull();
        assertThat(retrievedFile.getName()).isEqualTo("example.txt");
        assertThat(retrievedFile.getOwner()).isEqualTo("testOwner");
        assertThat(retrievedFile.getPath()).isEqualTo("/test-directory/example.txt");
        assertThat(retrievedFile.getSize()).isEqualTo(1024);
        assertThat(retrievedFile.isDirectory()).isFalse();
        assertThat(retrievedFile.getParentHash()).isEqualTo("parentHash123");
        assertThat(retrievedFile.getBlockHashes()).containsExactly("blockHash1");
    }

    @Test
    public void testListFiles() throws Exception {
        // Define the directory to list files from
        String directory = "/test-directory";

        // Create and save multiple test files in the specified directory
        DfsFile file1 = new DfsFile();
        file1.setHash("hash1234");
        file1.setName("example1.txt");
        file1.setOwner("testOwner");
        file1.setPath(directory + "/example1.txt");
        file1.setSize(512);
        file1.setDirectory(false);
        file1.setParentHash("parentHash123");
        file1.setBlockHashes(Collections.singletonList("blockHash1"));
        file1.setCreateTime(new Date());
        file1.setLastModifiedTime(new Date());

        DfsFile file2 = new DfsFile();
        file2.setHash("hash5678");
        file2.setName("example2.txt");
        file2.setOwner("testOwner");
        file2.setPath(directory + "/example2.txt");
        file2.setSize(1024);
        file2.setDirectory(false);
        file2.setParentHash("parentHash123");
        file2.setBlockHashes(Collections.singletonList("blockHash2"));
        file2.setCreateTime(new Date());
        file2.setLastModifiedTime(new Date());

        // Save the test files using the helper method
        saveTestFile(file1, directory);
        saveTestFile(file2, directory);

        // Create a request object for the directory
        FileTreeController.RequestDirectory request = new FileTreeController.RequestDirectory();
        request.setDirectory(directory);
        
        // Create the headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<FileTreeController.RequestDirectory> requestEntity = new HttpEntity<>(request, headers);

        // Send the POST request to list the files in the specified directory
        try {
            ResponseEntity<List<DfsFile>> response = restTemplate.exchange(
                    baseUrl + "/metadata/file/list", 
                    HttpMethod.POST, 
                    requestEntity, 
                    new ParameterizedTypeReference<List<DfsFile>>() {}
            );
            
            // Process the response if needed
            List<DfsFile> files = response.getBody();
            // ... do something with files
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            
            List<DfsFile> retrievedFiles = response.getBody();
            assertThat(retrievedFiles).isNotNull();
            assertThat(retrievedFiles).hasSize(2);
            assertThat(retrievedFiles).extracting(DfsFile::getName).containsExactlyInAnyOrder("example1.txt", "example2.txt");
            assertThat(retrievedFiles).extracting(DfsFile::getOwner).contains("testOwner");

        } catch (RestClientException e) {
            // Log the exception message
            System.err.println("Error during REST call: " + e.getMessage());
            
            // Check if there's a response body and log it
            if (e instanceof HttpClientErrorException) {
                HttpClientErrorException httpError = (HttpClientErrorException) e;
                System.err.println("Status code: " + httpError.getStatusCode());
                System.err.println("Response body: " + httpError.getResponseBodyAsString());
            }
        }

        // Verify the response status and that the list contains the files we saved
    }

    @Test
    public void testClearAllData() {
        // First, add a file to ensure there's data to clear
        DfsFile testFile = new DfsFile();
        testFile.setHash("hash123");
        testFile.setName("example.txt");
        testFile.setOwner("testOwner");
        testFile.setPath("/test-directory/example.txt");
        testFile.setSize(1024);
        testFile.setDirectory(false);
        testFile.setParentHash("parentHash123");
        testFile.setBlockHashes(Collections.singletonList("blockHash1"));
        testFile.setCreateTime(new Date());
        testFile.setLastModifiedTime(new Date());

        // Save the test file
        saveTestFile(testFile, "/test-directory");

        // Now, make a DELETE request to the clear all data endpoint
        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl + "/metadata/file/clear-all-data", 
                HttpMethod.DELETE, 
                null, 
                String.class
        );

        // Verify the response status
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("All dfs files cleard.");

        // Attempt to retrieve the previously saved file to ensure it has been cleared
        ResponseEntity<DfsFile> fileResponse = restTemplate.getForEntity(
                baseUrl + "/metadata/file/hash123", DfsFile.class);

        // Verify that the file no longer exists
        assertThat(fileResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    }
    
    @Test
    public void testGetBlockNodesListByFileHash() throws NoSuchAlgorithmException {
        // Create and save a test file with blocks
        DfsFile testFile = new DfsFile();
        String targetDirectory = "/test-directory";
        String fileHash = "hash123";
        testFile.setHash(fileHash);
        testFile.setName("example.txt");
        testFile.setOwner("testOwner");
        testFile.setPath("/test-directory/example.txt");
        testFile.setSize(1024);
        testFile.setDirectory(false);
        testFile.setParentHash("parentHash123");
        testFile.setBlockHashes(List.of("blockHash1", "blockHash2")); // Adding multiple block hashes
        testFile.setCreateTime(new Date());
        testFile.setLastModifiedTime(new Date());

        // Save the test file in your persistent store
        fileTreeManager.saveFile(testFile, targetDirectory);

        // Register the block locations
        blockMetaService.registerBlockLocation("blockHash1", "node1");
        blockMetaService.registerBlockLocation("blockHash2", "node2");
        String requestBody = fileHash;

        ResponseEntity<List<BlockNode>> response = restTemplate.exchange(
                baseUrl + "/metadata/file/block-nodes",  // Updated endpoint URL
                HttpMethod.POST,
                new HttpEntity<>(requestBody),  // Wrap the requestBody in HttpEntity
                new ParameterizedTypeReference<List<BlockNode>>() {}
        );
		
        // Verify the response
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(2);

        // Check the first block node
        BlockNode blockNode1 = response.getBody().get(0);
        assertThat(blockNode1.getHash()).isEqualTo("blockHash1");
        assertThat(blockNode1.getNodeUrls()).contains("node1"); // Check for the expected URL

        // Check the second block node
        BlockNode blockNode2 = response.getBody().get(1);
        assertThat(blockNode2.getHash()).isEqualTo("blockHash2");
        assertThat(blockNode2.getNodeUrls()).contains("node2"); // Check for the expected URL
        
    }


}
