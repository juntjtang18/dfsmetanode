package com.infolink.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.infolink.dfs.metanode.mdb.BlockNode;
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
    private RedisTemplate<String, DfsFile> redisFileRepo;
    @Autowired
    private BlockMetaService blockMetaService;
    
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

        if (redisFileRepo.hasKey(fileKey)) {
            throw new IllegalArgumentException("A file with the same name already exists in the target directory.");
        }

        // Log before saving
        //logger.info("Before saving to Redis: key = {}, value = {}", fileKey, dfsFile);
        
        // Store the DfsFile directly in Redis
        redisFileRepo.opsForValue().set(fileKey, dfsFile);
        redisFileRepo.opsForValue().set(dfsFile.getHash(), dfsFile);

        // Log after saving
        //logger.info("After saving to Redis: key = {}", fileKey);
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

            DfsFile existingDir = redisFileRepo.opsForValue().get(dirKey);
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

                redisFileRepo.opsForValue().set(dirKey, newDir);
                redisFileRepo.opsForValue().set(newDir.getHash(), newDir);
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
        Set<String> keys = redisFileRepo.keys(dirKey + "/*");
        
    	logger.info("in redistTemplate.keys({}:", dirKey+"/*");
        for(String key : keys) {
        	logger.info("key: {}", key);
        }
        
        logger.info("listFilesInDirectory(...) dirKey={}", dirKey+"/*");
        
        if (keys != null && !keys.isEmpty()) {
            keys.forEach(key -> {
                DfsFile file = redisFileRepo.opsForValue().get(key);
                if (file != null) {
                    files.add(file);
                }
            });
        }

        return files;
    }

    public void logAllKeys() {
        Set<String> keys = redisFileRepo.keys("*"); // Use wildcard "*" to match all keys

        logger.info("Logging all keys in Redis:");
        if (keys != null && !keys.isEmpty()) {
            for (String key : keys) {
                logger.info("Key: {}", key);
            }
        } else {
            logger.info("No keys found in Redis.");
        }
    }
    
    public boolean checkFileExistsByPath(String filePath) {
    	
    	return getFileByPath(filePath) != null; 
    }
    
    public boolean checkFileExistsByHash(String hash) {
    	return getFileByHash(hash) != null;
    }
    
    public DfsFile getFileByPath(String filePath) {
        String fileKey = FILE_PREFIX + filePath;
        return redisFileRepo.opsForValue().get(fileKey);
    }
    
    public DfsFile getFileByHash(String hash) {
    	String hashkey = HASH_PREFIX + hash;
    	return redisFileRepo.opsForValue().get(hashkey);
    }
    
    public List<BlockNode> getBlockNodesListByHash(String fileHash) throws Exception {
    	String hashkey = HASH_PREFIX + fileHash;
    	DfsFile dfsFile = redisFileRepo.opsForValue().get(hashkey);
    	List<String> blockHashes = dfsFile.getBlockHashes();
    	List<BlockNode> blockNodeList = new ArrayList<BlockNode>();
    	for (String blockHash : blockHashes) {
    		// get the nodes for the blockHash.
    		BlockNode blockNode = blockMetaService.getBlockNodeByHash(blockHash);
    		if (blockNode == null) {
    			throw new Exception("File with hash{" + fileHash + "} broken.");
    		}
    		blockNodeList.add(blockNode);
    	}
    	return blockNodeList;
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
        Set<String> keys = redisFileRepo.keys("*");

        if (keys != null && !keys.isEmpty()) {
            redisFileRepo.delete(keys);
            logger.info("Cleared all keys in Redis for testing purposes.");
        } else {
            logger.info("No keys found to clear in Redis.");
        }
    }
    
    public void deleteByHash(String fileHash) {
        String hashKey = HASH_PREFIX + fileHash;
        DfsFile dfsFile = redisFileRepo.opsForValue().get(hashKey);
        if (dfsFile != null) {
            String fileKey = FILE_PREFIX + dfsFile.getPath();
            redisFileRepo.delete(hashKey);
            redisFileRepo.delete(fileKey); // Also delete by path
            logger.info("Deleted file with hash: {}", fileHash);
        } else {
            logger.warn("No file found with hash: {}", fileHash);
        }
    }

    public void deleteByPath(String filePath) {
        String fileKey = FILE_PREFIX + filePath;
        DfsFile dfsFile = redisFileRepo.opsForValue().get(fileKey);
        if (dfsFile != null) {
            // Delete the file from Redis using its path
        	String hashKey = HASH_PREFIX + dfsFile.getHash();
            redisFileRepo.delete(fileKey);
            redisFileRepo.delete(hashKey); // Also delete by hash
            logger.info("Deleted file with path: {}", filePath);
        } else {
            logger.warn("No file found with path: {}", filePath);
        }
    }
}
