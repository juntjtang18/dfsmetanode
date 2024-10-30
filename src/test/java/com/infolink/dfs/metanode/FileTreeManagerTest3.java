package com.infolink.dfs.metanode;

import static org.junit.jupiter.api.Assertions.*;

import com.infolink.dfs.shared.DfsFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@SpringBootTest
public class FileTreeManagerTest3 {

    @Autowired
    private FileTreeManager fileTreeManager;

    @Autowired
    private RedisTemplate<String, DfsFile> redisFileRepo;

    private final String owner = "testOwner";

    @BeforeEach
    public void setup() {
        // Clear Redis before each test
        fileTreeManager.clearAllData();
    }

    @AfterEach
    public void cleanup() {
        // Clear Redis after each test
        fileTreeManager.clearAllData();
    }

    @Test
    public void testCreateDirectoriesRecursivelyAndVerifyFilesAndSubdirectories() throws NoSuchAlgorithmException {
        // Define block hashes for files
        List<String> blockHashes = Arrays.asList("blockHash1", "blockHash2");

        // Create DfsFile objects with updated full paths
        DfsFile dfsFile1 = new DfsFile("hash1", owner, "file1.txt", "/layer1/layer2A/file1.txt", 1024, false, "parentHash1", blockHashes);
        DfsFile dfsFile2 = new DfsFile("hash2", owner, "file2.txt", "/layer1/layer2A/file2.txt", 2048, false, "parentHash1", blockHashes);
        DfsFile dfsFile3 = new DfsFile("hash3", owner, "file3.txt", "/layer1/layer2A/file3.txt", 4096, false, "parentHash1", blockHashes);
        DfsFile dfsFile4 = new DfsFile("hash4", owner, "file4.txt", "/layer1/layer2B/file4.txt", 1024, false, "parentHash2", blockHashes);
        DfsFile dfsFile5 = new DfsFile("hash5", owner, "file5.txt", "/layer1/layer2B/file5.txt", 2048, false, "parentHash2", blockHashes);

        // Directory paths
        String pathA = "/layer1/layer2A";
        String pathB = "/layer1/layer2B";
        
        // Save files to layer2A and layer2B
        fileTreeManager.createDirectoriesRecursively(pathA, owner);
        fileTreeManager.createDirectoriesRecursively(pathB, owner);

        fileTreeManager.saveFile(dfsFile1, pathA);
        fileTreeManager.saveFile(dfsFile2, pathA);
        fileTreeManager.saveFile(dfsFile3, pathA);

        fileTreeManager.saveFile(dfsFile4, pathB);
        fileTreeManager.saveFile(dfsFile5, pathB);

        // Create subdirectories under /layer1/layer2A
        String layer3APath = "/layer1/layer2A/layer3A";
        String layer3BPath = "/layer1/layer2A/layer3B";
        
        DfsFile layer3A = new DfsFile("hash6", owner, "layer3A", layer3APath + "/layer3A", 0, true, "parentHash1", null);
        DfsFile layer3B = new DfsFile("hash7", owner, "layer3B", layer3BPath + "/layer3B", 0, true, "parentHash1", null);

        fileTreeManager.createDirectoriesRecursively(layer3APath, owner);
        fileTreeManager.createDirectoriesRecursively(layer3BPath, owner);

        // Verify files and subdirectories in /layer1/layer2A
        validateFilesInDirectory(pathA, "file1.txt", "file2.txt", "file3.txt");
        validateSubdirectoriesInDirectory(pathA, "layer3A", "layer3B");

        // Verify contents using listFilesInDirectory
        List<DfsFile> directoryContents = fileTreeManager.listFilesInDirectory(pathA);
        List<String> actualNames = directoryContents.stream().map(DfsFile::getName).collect(Collectors.toList());

        // Expected order: directories first, then files
        List<String> expectedNames = Arrays.asList("layer3A", "layer3B", "file1.txt", "file2.txt", "file3.txt");
        assertEquals(expectedNames, actualNames, "Directory contents do not match expected order and names");
    }

    // Helper method to validate file presence in Redis for a specific directory
    private void validateFilesInDirectory(String directoryPath, String... expectedFileNames) {
        String dirKey = FileTreeManager.DIR_PREFIX + directoryPath + ":files";
        Set<DfsFile> files = redisFileRepo.opsForSet().members(dirKey);
        
        assertNotNull(files, "Expected files set not found in Redis for directory: " + directoryPath);
        
        List<String> fileNames = files.stream().map(DfsFile::getName).collect(Collectors.toList());
        for (String expectedFileName : expectedFileNames) {
            assertTrue(fileNames.contains(expectedFileName), "Expected file not found: " + expectedFileName);
        }
    }

    // Helper method to validate subdirectory presence in Redis for a specific directory
    private void validateSubdirectoriesInDirectory(String directoryPath, String... expectedSubdirs) {
        String dirKey = FileTreeManager.DIR_PREFIX + directoryPath;
        Set<DfsFile> subdirectories = redisFileRepo.opsForSet().members(dirKey + ":dir");

        assertNotNull(subdirectories, "Expected subdirectories set not found in Redis for directory: " + directoryPath);
        
        List<String> subdirNames = subdirectories.stream().map(DfsFile::getName).collect(Collectors.toList());
        for (String expectedSubdir : expectedSubdirs) {
            assertTrue(subdirNames.contains(expectedSubdir), "Expected subdirectory not found: " + expectedSubdir);
        }
    }
    
    @Test
    public void testCreateDirectoryCreatesTargetDirectoryAndReturnsHash() throws NoSuchAlgorithmException {
        // Test creating a single-level directory within root
        String directory = "/testDir";
        String parentDirectory = "/";
        String finalDirHash = fileTreeManager.createDirectory(directory, parentDirectory, owner);

        // Verify the directory was created
        String dirKey = FileTreeManager.DIR_PREFIX + directory;
        DfsFile createdDir = redisFileRepo.opsForValue().get(dirKey);
        assertNotNull(createdDir, "Directory was not created in Redis");
        assertEquals(finalDirHash, createdDir.getHash(), "Hash does not match the expected hash of the final directory");
    }

    @Test
    public void testCreateDirectoryWithNestedDirectories() throws NoSuchAlgorithmException {
        // Test creating nested directories
        String directory = "/nestedDir1/nestedDir2";
        String parentDirectory = "/";
        String finalDirHash = fileTreeManager.createDirectory(directory, parentDirectory, owner);

        // Verify each directory level was created
        String dirKeyLevel1 = FileTreeManager.DIR_PREFIX + "/nestedDir1";
        String dirKeyLevel2 = FileTreeManager.DIR_PREFIX + "/nestedDir1/nestedDir2";

        DfsFile level1Dir = redisFileRepo.opsForValue().get(dirKeyLevel1);
        DfsFile level2Dir = redisFileRepo.opsForValue().get(dirKeyLevel2);

        assertNotNull(level1Dir, "Level 1 directory was not created");
        assertNotNull(level2Dir, "Level 2 directory was not created");
        assertEquals(finalDirHash, level2Dir.getHash(), "Hash does not match the expected hash of the final directory");
    }

    @Test
    public void testCreateDirectoryWithExistingParent() throws NoSuchAlgorithmException {
        // Create a parent directory first
        String parentDirectory = "/existingParent";
        fileTreeManager.createDirectoriesRecursively(parentDirectory, owner);

        // Create a new directory under the existing parent
        String directory = "/newSubDir";
        String finalDirHash = fileTreeManager.createDirectory(directory, parentDirectory, owner);

        // Verify the new directory was created under the existing parent
        String dirKey = FileTreeManager.DIR_PREFIX + parentDirectory + directory;
        DfsFile newDir = redisFileRepo.opsForValue().get(dirKey);
        assertNotNull(newDir, "Subdirectory was not created under the existing parent directory");
        assertEquals(finalDirHash, newDir.getHash(), "Hash does not match the expected hash of the final directory");
    }

    @Test
    public void testCreateDirectoryReturnsCorrectHashForDeepNesting() throws NoSuchAlgorithmException {
        // Test creating deeply nested directories
        String directory = "/deep/nest/dir/structure";
        String parentDirectory = "/";
        String finalDirHash = fileTreeManager.createDirectory(directory, parentDirectory, owner);

        // Verify the final directory's hash
        String dirKey = FileTreeManager.DIR_PREFIX + directory;
        DfsFile createdDir = redisFileRepo.opsForValue().get(dirKey);
        assertNotNull(createdDir, "Deeply nested directory was not created");
        assertEquals(finalDirHash, createdDir.getHash(), "Hash does not match the expected hash of the final directory for deep nesting");
    }
    @Test
    public void testListRootDirectory() throws NoSuchAlgorithmException {
        // Create directories at the root level
        String layer1APath = "/layer1A/layer2A";
        String layer1BPath = "/layer1B/layer2B";

        // Create directories recursively
        fileTreeManager.createDirectoriesRecursively(layer1APath, owner);
        fileTreeManager.createDirectoriesRecursively(layer1BPath, owner);

        // Define and save files in the root directory
        DfsFile testFile1 = new DfsFile("hash6", owner, "testfile1.txt", "/testfile1.txt", 1024, false, "parentHash1", null);
        DfsFile testFile2 = new DfsFile("hash7", owner, "testfile2.txt", "/testfile2.txt", 2048, false, "parentHash1", null);

        // Save files to root
        fileTreeManager.saveFile(testFile1, "/");
        fileTreeManager.saveFile(testFile2, "/");

        // Verify subdirectories and files in the root directory
        validateSubdirectoriesInDirectory("/", "layer1A", "layer1B");
        validateFilesInDirectory("/", "testfile1.txt", "testfile2.txt");

        // Verify contents using listFilesInDirectory
        List<DfsFile> rootContents = fileTreeManager.listFilesInDirectory("/");
        List<String> actualRootNames = rootContents.stream().map(DfsFile::getName).collect(Collectors.toList());

        // Expected order: directories first, then files
        List<String> expectedRootNames = Arrays.asList("layer1A", "layer1B", "testfile1.txt", "testfile2.txt");
        assertEquals(expectedRootNames, actualRootNames, "Root directory contents do not match expected order and names");
    }
    
}
