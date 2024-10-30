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
import java.util.List;
import java.util.Set;

@SpringBootTest
public class FileTreeManagerTest2 {

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
    public void testCreateDirectoriesRecursively_singlePath() throws NoSuchAlgorithmException {
        String directoryPath = "/layer1/layer2/layer3/layer4";
        
        fileTreeManager.createDirectoriesRecursively(directoryPath, owner);

        // Validate each directory level exists in Redis
        validateDirectory("/layer1", "layer2");
        validateDirectory("/layer1/layer2", "layer3");
        validateDirectory("/layer1/layer2/layer3", "layer4");
    }

    @Test
    public void testCreateDirectoriesRecursively_multipleSubdirectories() throws NoSuchAlgorithmException {
        String pathA = "/layer1/layer2A/layer3";
        String pathB = "/layer1/layer2B/layer3";

        fileTreeManager.createDirectoriesRecursively(pathA, owner);
        fileTreeManager.createDirectoriesRecursively(pathB, owner);

        // Validate root-level subdirectories
        validateDirectory("/layer1", "layer2A", "layer2B");

        // Validate deeper subdirectories
        validateDirectory("/layer1/layer2A", "layer3");
        validateDirectory("/layer1/layer2B", "layer3");
    }

    private void validateDirectory(String parentPath, String... expectedSubdirs) {
        String dirKey = FileTreeManager.DIR_PREFIX + parentPath;
        DfsFile parentDir = redisFileRepo.opsForValue().get(dirKey);
        
        assertNotNull(parentDir, "Expected directory not found in Redis: " + parentPath);
        Set<DfsFile> subdirectories = redisFileRepo.opsForSet().members(dirKey + ":dir");

        for (String expectedSubdir : expectedSubdirs) {
            boolean found = subdirectories.stream().anyMatch(dfsFile -> dfsFile.getName().equals(expectedSubdir));
            assertTrue(found, "Expected subdirectory not found: " + expectedSubdir + " under " + parentPath);
        }
    }
}
