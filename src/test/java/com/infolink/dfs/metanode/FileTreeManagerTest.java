package com.infolink.dfs.metanode;

import com.infolink.dfs.shared.DfsFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test") // Assuming you have a test profile set up
public class FileTreeManagerTest {

    @Autowired
    private FileTreeManager fileTreeManager;

    @Autowired
    private RedisTemplate<String, DfsFile> redisTemplate;

    private final String testDirectory = "/testDir";
    private final String testFileName = "testFile.txt";
    private final String testOwner = "testOwner";

    @BeforeEach
    void setUp() {
        // Clear any existing data before each test
        fileTreeManager.clearAllData();
    }

    @AfterEach
    void tearDown() {
        // Clean up after each test
        fileTreeManager.clearAllData();
    }

    @Test
    void testSaveFile() throws NoSuchAlgorithmException {
        DfsFile testFile = new DfsFile("testHash", testOwner, testFileName, testDirectory + "/" + testFileName, 0L, false, null, List.of());
        fileTreeManager.saveFile(testFile, testDirectory);

        // Verify the file was saved correctly
        DfsFile retrievedFile = redisTemplate.opsForValue().get(testDirectory + "/" + testFileName);
        assertNotNull(retrievedFile);
        assertEquals(testFileName, retrievedFile.getName());
    }

    @Test
    void testListFilesInDirectory() throws NoSuchAlgorithmException {
        DfsFile file1 = new DfsFile("hash1", testOwner, "file1.txt", testDirectory + "/file1.txt", 0L, false, null, List.of());
        DfsFile file2 = new DfsFile("hash2", testOwner, "file2.txt", testDirectory + "/file2.txt", 0L, false, null, List.of());

        fileTreeManager.saveFile(file1, testDirectory);
        fileTreeManager.saveFile(file2, testDirectory);

        // List files in the directory
        List<DfsFile> files = fileTreeManager.listFilesInDirectory(testDirectory);
        assertEquals(2, files.size());
    }

    @Test
    void testCreateDirectoriesRecursively() throws NoSuchAlgorithmException {
        String nestedDirectory = testDirectory + "/subDir1/subDir2";
        fileTreeManager.createDirectoriesRecursively(nestedDirectory, testOwner);

        // Check if the directories are created
        assertNotNull(redisTemplate.opsForValue().get(nestedDirectory));
    }

    @Test
    void testCreateMultipleSubdirectories() throws NoSuchAlgorithmException {
        String nestedPath = testDirectory + "/subDir1/subDir2/subDir3";

        // Create directories recursively
        fileTreeManager.createDirectoriesRecursively(nestedPath, testOwner);

        // Verify that each directory exists
        String[] pathSegments = nestedPath.split("/");
        StringBuilder currentPath = new StringBuilder();

        for (String segment : pathSegments) {
            if (segment.isEmpty()) continue; // Skip empty segments
            currentPath.append("/").append(segment);
            
            // Check if the current directory exists in Redis
            DfsFile dir = redisTemplate.opsForValue().get(currentPath.toString());
            assertNotNull(dir, "Directory " + currentPath.toString() + " should exist");
            assertTrue(dir.isDirectory(), "The path " + currentPath.toString() + " should be a directory");
        }
    }

    @Test
    void testGetFile() throws NoSuchAlgorithmException {
        DfsFile testFile = new DfsFile("testHash", testOwner, testFileName, testDirectory + "/" + testFileName, 0L, false, null, List.of());
        fileTreeManager.saveFile(testFile, testDirectory);

        DfsFile retrievedFile = fileTreeManager.getFile(testDirectory + "/" + testFileName);
        assertNotNull(retrievedFile);
        assertEquals(testFileName, retrievedFile.getName());
    }

    @Test
    void testClearAllData() throws NoSuchAlgorithmException {
        DfsFile testFile = new DfsFile("testHash", testOwner, testFileName, testDirectory + "/" + testFileName, 0L, false, null, List.of());
        fileTreeManager.saveFile(testFile, testDirectory);

        // Ensure data exists before clearing
        assertNotNull(redisTemplate.opsForValue().get(testDirectory + "/" + testFileName));

        // Clear all data
        fileTreeManager.clearAllData();

        // Ensure data does not exist after clearing
        assertNull(redisTemplate.opsForValue().get(testDirectory + "/" + testFileName));
    }
}
