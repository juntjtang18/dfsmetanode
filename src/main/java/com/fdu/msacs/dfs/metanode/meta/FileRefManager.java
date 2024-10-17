package com.fdu.msacs.dfs.metanode.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.*;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FileRefManager {
    private final Path refRoot;
    @Autowired
    private final ObjectMapper objectMapper;

    public FileRefManager(String repositoryRoot) {
        this.refRoot = Paths.get(repositoryRoot, "refs");
        this.objectMapper = new ObjectMapper(); // Initialize the ObjectMapper
    }

 // Method to save hash and BackupFile to a reference file
    public String saveHashMapping(String hash, DfsFile dfsFile) throws IOException {
        Path refFile = getFilePathFromHash(hash);  // Get the full path from hash
        Files.createDirectories(refFile.getParent());  // Create directory if it doesn't exist

        // Write the BackupFile to the reference file as JSON
        objectMapper.writeValue(refFile.toFile(), dfsFile);
        return hash;
    }
    
    private Path getFilePathFromHash(String hash) {
        int folderSegmentLength = 2;  // Length of each directory segment
        int numFolders = 4;           // Number of folder levels

        Path refDirectory = refRoot;
        for (int i = 0; i < numFolders; i++) {
            refDirectory = refDirectory.resolve(hash.substring(i * folderSegmentLength, (i + 1) * folderSegmentLength));
        }

        // Remaining part of the hash used as the file name (after directory segments)
        String fileName = hash.substring(numFolders * folderSegmentLength) + ".json";  // Change extension to .json
        return refDirectory.resolve(fileName);  // Return the complete file path
    }

    // Method to check if a hash exists and retrieve the BackupFile
    public Optional<DfsFile> readHashMapping(String hash) throws IOException {
        Path refFile = getFilePathFromHash(hash);

        if (Files.exists(refFile)) {
            // Read the BackupFile from the reference file
            DfsFile backupFile = objectMapper.readValue(refFile.toFile(), DfsFile.class);
            return Optional.of(backupFile);
        } else {
            return Optional.empty();  // Indicate that the hash does not exist
        }
    }

    public boolean fileHashExists(String hash) throws IOException {
        Path refFile = getFilePathFromHash(hash);
        return Files.exists(refFile);
    }


    // Main method for testing purposes
    public static void main(String[] args) {
        FileRefManager manager = new FileRefManager("./ref");

        String hash = "abc123456789defabcd5678abcdef12";
        Set<String> blocks = Set.of("block1", "block2", "block3");
        FileMeta fileMeta = new FileMeta("d:\\mybackupfiles\\a\\f.txt", 0, hash); // Initialize your FileMeta here

        DfsFile backupFile = new DfsFile(fileMeta, blocks);

        try {
            // Save hash mapping
            manager.saveHashMapping(hash, backupFile);
            System.out.println("Hash mapping saved successfully.");

            // Check hash mapping
            Optional<DfsFile> result = manager.readHashMapping(hash);
            if (result.isPresent()) {
                DfsFile retrievedBackupFile = result.get();
                System.out.println("Backup File Metadata: " + retrievedBackupFile.getFileMeta());
                System.out.println("Block Mapping: " + retrievedBackupFile.getBlockHashes());
            } else {
                System.out.println("Hash mapping not found.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

