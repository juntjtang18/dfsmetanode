package com.infolink.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.infolink.dfs.shared.DfsFile;
import com.infolink.dfs.shared.HashUtil;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

@Service
public class FileTreeManager {
    private static final Logger logger = LoggerFactory.getLogger(FileTreeManager.class);
    
    private static final String DIR_PREFIX = ""; 
    private static final String FILE_PREFIX = "";
    private static final String HASH_PREFIX = "";
    
    @Autowired
    private RedisTemplate<String, DfsFile> redisTemplate;

    public String saveFile(DfsFile dfsFile, String targetDirectory) throws NoSuchAlgorithmException {
        // Validate input parameters
        if (dfsFile == null) {
            throw new IllegalArgumentException("DfsFile cannot be null");
        }

        String fileName = dfsFile.getName();
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be empty");
        }

        if (fileName.length() > 255) {
            throw new IllegalArgumentException("File name cannot exceed 255 characters");
        }

        if (targetDirectory == null || targetDirectory.trim().isEmpty()) {
            throw new IllegalArgumentException("Target directory cannot be null or empty");
        }

        if (!targetDirectory.matches("^[^<>|:\"*?]*$")) {
            throw new IllegalArgumentException("Target directory contains invalid characters");
        }

        String owner = dfsFile.getOwner();
        if (owner == null || owner.trim().isEmpty()) {
            throw new IllegalArgumentException("File owner cannot be null or empty");
        }

        createDirectoriesRecursively(targetDirectory, owner);

        //String filePath = targetDirectory + "/" + fileName;
        String fileKey = FILE_PREFIX + targetDirectory + "/" + fileName; // Directly use filePath as the key

        if (redisTemplate.hasKey(fileKey)) {
            throw new IllegalArgumentException("A file with the same name already exists in the target directory.");
        }

        // Log before saving
        logger.info("Before saving to Redis: key = {}, value = {}", fileKey, dfsFile);
        
        // Store the DfsFile directly in Redis
        redisTemplate.opsForValue().set(fileKey, dfsFile);
        redisTemplate.opsForValue().set(dfsFile.getHash(), dfsFile);

        // Log after saving
        logger.info("After saving to Redis: key = {}", fileKey);
        return dfsFile.getHash();
    }

    void createDirectoriesRecursively(String path, String owner) throws NoSuchAlgorithmException {
        String[] pathSegments = path.split("/");
        StringBuilder currentPath = new StringBuilder();
        String parentId = null;

        for (String segment : pathSegments) {
            if (segment.isEmpty()) continue; 
            currentPath.append("/").append(segment);

            String currentPathStr = currentPath.toString();
            String dirKey = DIR_PREFIX + currentPathStr;

            DfsFile existingDir = redisTemplate.opsForValue().get(dirKey);
            if (existingDir == null) {
                String dirHash = generateHashForPath(currentPathStr);
                DfsFile newDir = new DfsFile(
                    dirHash,
                    owner,
                    segment,
                    currentPathStr,
                    0L,
                    true,
                    parentId,
                    List.of()
                );
                newDir.setCreateTime(new Date());
                newDir.setLastModifiedTime(new Date());

                redisTemplate.opsForValue().set(dirKey, newDir);
                redisTemplate.opsForValue().set(newDir.getHash(), newDir);
                parentId = dirHash;
            } else {
                parentId = existingDir.getHash();
            }
        }
    }

    public List<DfsFile> listFilesInDirectory(String directory) {
    	
        if (directory == null || directory.trim().isEmpty()) {
            throw new IllegalArgumentException("Directory cannot be null or empty");
        }
        
        logAllKeys();
        
        String dirKey = FILE_PREFIX + directory;
        List<DfsFile> files = new ArrayList<>();

        // Using wildcard to match files in the specified directory
        Set<String> keys = redisTemplate.keys(dirKey + "/*");
        
    	logger.info("in redistTemplate.keys({}:", dirKey+"/*");
        for(String key : keys) {
        	logger.info("key: {}", key);
        }
        
        logger.info("listFilesInDirectory(...) dirKey={}", dirKey+"/*");
        
        if (keys != null && !keys.isEmpty()) {
            keys.forEach(key -> {
                DfsFile file = redisTemplate.opsForValue().get(key);
                if (file != null) {
                    files.add(file);
                }
            });
        }

        return files;
    }

    public void logAllKeys() {
        Set<String> keys = redisTemplate.keys("*"); // Use wildcard "*" to match all keys

        logger.info("Logging all keys in Redis:");
        if (keys != null && !keys.isEmpty()) {
            for (String key : keys) {
                logger.info("Key: {}", key);
            }
        } else {
            logger.info("No keys found in Redis.");
        }
    }
    
    public boolean checkFileExists(String filePath) {
    	
    	return getFile(filePath) != null; 
    }

    public DfsFile getFile(String filePath) {
        String fileKey = FILE_PREFIX + filePath;
        return redisTemplate.opsForValue().get(fileKey);
    }

    /*
     * Here the hash for the path is temporary method to calculate. 
     * In future, need to think about that a folder's hash should be calculated based on files and subfolders' hash.
     * This is for backup file system purpose.
     */
    String generateHashForPath(String path) throws NoSuchAlgorithmException {
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
        return HashUtil.calculateHash(pathBytes);
    }
    
    /**
     * Clears all data in Redis.
     * This method should be used with caution and is primarily intended for testing purposes.
     */
    public void clearAllData() {
        Set<String> keys = redisTemplate.keys("*");

        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
            logger.info("Cleared all keys in Redis for testing purposes.");
        } else {
            logger.info("No keys found to clear in Redis.");
        }
    }
}
