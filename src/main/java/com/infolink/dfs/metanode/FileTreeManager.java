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
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;

@Service
public class FileTreeManager {
    private static final Logger logger = LoggerFactory.getLogger(FileTreeManager.class);
    
    static final String DIR_PREFIX = "dir:"; 
    static final String FILE_PREFIX = "file:";
    static final String HASH_PREFIX = "hash:";
    
    @Autowired
    private RedisTemplate<String, DfsFile> redisFileRepo;
    @Autowired
    private BlockMetaService blockMetaService;
    
    public String saveFile(DfsFile dfsFile, String targetDirectory) throws NoSuchAlgorithmException {
    	if (!targetDirectory.startsWith("/")) targetDirectory = "/" + targetDirectory;
    	
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
        String fileKey;
        if (targetDirectory.equals("/")) {
        	fileKey 		= FILE_PREFIX + "/" + fileName; 
        } else {
        	fileKey 		= FILE_PREFIX + targetDirectory + "/" + fileName; 
        }
        String parentDirKey = DIR_PREFIX  + targetDirectory;
        String hashKey		= HASH_PREFIX + dfsFile.getHash();
        
        if (redisFileRepo.hasKey(fileKey)) {
            throw new IllegalArgumentException("A file with the same name already exists in the target directory.");
        }

        // Log before saving
        //logger.info("Before saving to Redis: key = {}, value = {}", fileKey, dfsFile);
        
        // Store the DfsFile directly in Redis
        redisFileRepo.opsForValue().set(fileKey, dfsFile);
        redisFileRepo.opsForValue().set(hashKey, dfsFile);
        redisFileRepo.opsForSet().add(parentDirKey + ":files", dfsFile);
        
        // Log after saving
        //logger.info("After saving to Redis: key = {}", fileKey);
        return dfsFile.getHash();
    }

    public String createDirectory(String directory, String parentDirectory, String owner) throws NoSuchAlgorithmException {
        // Ensure that directory and parentDirectory start with "/"
        if (!directory.startsWith("/")) directory = "/" + directory;
        if (!parentDirectory.startsWith("/")) parentDirectory = "/" + parentDirectory;

        String fullPath = parentDirectory.endsWith("/") ? parentDirectory + directory : parentDirectory + "/" + directory;

        // Create directories recursively up to the target directory and get the hash of the final directory
        String finalDirHash = createDirectoriesRecursively(fullPath, owner);

        logger.info("Created directory {} within parent directory {}", directory, parentDirectory);
        return finalDirHash;
    }

    public String createDirectoriesRecursively(String path, String owner) throws NoSuchAlgorithmException {
        if (!path.startsWith("/")) path = "/" + path;

        String[] pathSegments = path.split("/");

        logger.debug("Path={}, split segments are: {}", path, pathSegments);
        String rootDir = "/";
        String rootDirKey = DIR_PREFIX + rootDir;
        String rootHash = generateHashForPath(rootDir);
        if (redisFileRepo.opsForValue().get(rootDirKey) == null) {
            DfsFile rootDirDfsFile = new DfsFile(generateHashForPath("/"), owner, "/", "/", 0L, true, null, List.of());
            redisFileRepo.opsForValue().set(rootDirKey, rootDirDfsFile);
        }

        StringBuilder currentPath = new StringBuilder();
        String parentId = rootHash;
        String parentDir = rootDir;
        String finalDirHash = rootHash;

        for (String segment : pathSegments) {
            logger.debug("Creating directory: {}", segment);

            if (segment.isEmpty()) continue;
            currentPath.append("/").append(segment);

            String currentPathStr = currentPath.toString();
            String dirKey = DIR_PREFIX + currentPathStr;

            DfsFile existingDir = redisFileRepo.opsForValue().get(dirKey);
            if (existingDir == null) {
                String dirHash = generateHashForPath(currentPathStr);
                DfsFile newDir = new DfsFile(dirHash, owner, segment, currentPathStr, 0L, true, parentId, List.of());

                redisFileRepo.opsForValue().set(dirKey, newDir);
                String hashKey = HASH_PREFIX + dirHash;
                redisFileRepo.opsForValue().set(hashKey, newDir);

                logger.debug("Directory {} mapped to {}", dirKey, newDir);
                logger.debug("Hash {} mapped to {}", hashKey, newDir);

                if (parentId != null) {
                    String parentDirKey = DIR_PREFIX + parentDir;
                    redisFileRepo.opsForSet().add(parentDirKey + ":dir", newDir);
                    logger.debug("ParentDirKey={}", parentDirKey + ":dir");
                }
                parentId = dirHash;
                parentDir = currentPathStr;
                finalDirHash = dirHash;
            } else {
                parentId = existingDir.getHash();
                parentDir = currentPathStr;
                finalDirHash = parentId; // Update to the hash of the existing directory
            }
        }

        return finalDirHash; // Return the hash of the final directory
    }

    /**
     * List all files and subdirectories in a given directory, sorted by type.
     * Directories appear first, followed by files.
     *
     * @param directory the directory path to list contents from
     * @return a list of DfsFile objects representing the files and directories within the specified directory
     */
    public List<DfsFile> listFilesInDirectory(String directory) {
        List<DfsFile> filesAndDirectories = new ArrayList<>();

        // Fetch files
        Set<DfsFile> files = redisFileRepo.opsForSet().members(DIR_PREFIX + directory + ":files");
        if (files != null) {
            filesAndDirectories.addAll(files);
        }

        // Fetch subdirectories
        Set<DfsFile> subdirectories = redisFileRepo.opsForSet().members(DIR_PREFIX + directory + ":dir");
        if (subdirectories != null) {
            filesAndDirectories.addAll(subdirectories);
        }

        // Sort directories first, then files
        filesAndDirectories.sort(Comparator.comparing(DfsFile::isDirectory).reversed().thenComparing(DfsFile::getName));

        return filesAndDirectories;
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
    	if (!filePath.startsWith("/")) filePath = "/" + filePath;
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
