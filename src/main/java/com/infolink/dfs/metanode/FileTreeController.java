package com.infolink.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.infolink.dfs.metanode.mdb.BlockNode;
import com.infolink.dfs.shared.DfsFile;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class FileTreeController {
    private static final Logger logger = LoggerFactory.getLogger(FileTreeController.class);

    @Autowired
    private FileTreeManager fileTreeManager;
    @Autowired
    private BlockMetaService blockMetaService;

    /**
     * Endpoint to save a file.
     *
     * @param request The request containing the DfsFile and target directory.
     * @return ResponseEntity with status indicating success or failure.
     */
    @PostMapping("/metadata/file/save")
    public ResponseEntity<String> saveFile(@RequestBody RequestSaveFile request) {
        try {
            DfsFile dfsFile = request.getDfsFile();
            String targetDirectory = request.getTargetDirectory();
            
            fileTreeManager.saveFile(dfsFile, targetDirectory);
            return ResponseEntity.status(HttpStatus.CREATED).body("File saved successfully.");
        } catch (NoSuchAlgorithmException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error in file saving: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.FOUND).body(e.getMessage());
        }
    }

    /**
     * Endpoint to retrieve a file by its hash.
     *
     * @param hash The hash of the file to retrieve.
     * @return ResponseEntity with the file details or error message.
     */
    @GetMapping("/metadata/file/{hash}")
    public ResponseEntity<DfsFile> getFile(@PathVariable String hash) {
        DfsFile dfsFile = fileTreeManager.getFileByHash(hash);
        if (dfsFile != null) {
            return ResponseEntity.ok(dfsFile);
        } else {
            return ResponseEntity.status(HttpStatus.NO_CONTENT).body(null);
        }
    }
    
    @PostMapping("/metadata/file/get-by-filepath")
    public ResponseEntity<DfsFile> getByFilePath(@RequestBody Map<String, String> request) {
        String username = request.get("username");
        String filePath = request.get("filePath");

        // Use both username and filePath in file lookup
        DfsFile dfsFile = fileTreeManager.getFileByPath(filePath);
        if (dfsFile != null) {
            return ResponseEntity.ok(dfsFile);
        } else {
            return ResponseEntity.status(HttpStatus.NO_CONTENT).body(null);
        }
    }

    @PostMapping("/metadata/file/block-nodes")
    public ResponseEntity<List<BlockNode>> getBlockNodesListByFileHash(@RequestBody String fileHash) {
        logger.debug("Received request to get block nodes for file hash: {}", fileHash);

        DfsFile dfsFile = fileTreeManager.getFileByPath(fileHash);
        if (dfsFile == null) {
            logger.warn("No DfsFile found for hash: {}", fileHash);
            return ResponseEntity.status(HttpStatus.NO_CONTENT).body(null);
        }

        try {
            logger.info("Fetching block nodes for file hash: {}", fileHash);
            List<BlockNode> blockNodeList = fileTreeManager.getBlockNodesListByHash(fileHash);
            logger.info("Successfully retrieved {} block nodes for file hash: {}", blockNodeList.size(), fileHash);
            return ResponseEntity.status(HttpStatus.OK).body(blockNodeList);
        } catch (Exception e) {
            logger.error("Error occurred while retrieving block nodes for file hash: {}", fileHash, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
    
    /**
     * Endpoint to list files in a directory.
     *
     * @param request The request containing the directory to list files from.
     * @return ResponseEntity with the list of files or error message.
     */
    @PostMapping("/metadata/file/list")
    public ResponseEntity<List<DfsFile>> listFiles(@RequestBody RequestDirectory request) {
        logger.info("/metadata/file/list requested for directory: {}", request.getDirectory());
        
        List<DfsFile> files = fileTreeManager.listFilesInDirectory(request.getDirectory());
        // Log the files before returning
        logger.info("Returning files: {}", files);
        return ResponseEntity.ok(files);
    }
    
    @DeleteMapping("/metadata/file/clear-all-data")
    public ResponseEntity<String> clearAllData() {
    	fileTreeManager.clearAllData();
    	return ResponseEntity.ok("All dfs files cleard.");
    }
    /**
     * Inner class to represent a request containing the DfsFile and target directory.
     */
    public static class RequestSaveFile {
        private DfsFile dfsFile;
        private String targetDirectory;

        // Getters and Setters
        public DfsFile getDfsFile() {
            return dfsFile;
        }

        public void setDfsFile(DfsFile dfsFile) {
            this.dfsFile = dfsFile;
        }

        public String getTargetDirectory() {
            return targetDirectory;
        }

        public void setTargetDirectory(String targetDirectory) {
            this.targetDirectory = targetDirectory;
        }
    }

    /**
     * Inner class to represent a request containing the directory.
     */
    public static class RequestDirectory {
        private String directory;

        // Getters and Setters
        public String getDirectory() {
            return directory;
        }

        public void setDirectory(String directory) {
            this.directory = directory;
        }
    }
}
