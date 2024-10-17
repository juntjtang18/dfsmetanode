package com.fdu.msacs.dfs.metanode;

import com.fdu.msacs.dfs.metanode.meta.DfsFile;
import com.fdu.msacs.dfs.metanode.meta.FileRefManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Optional;

@RestController
public class DfsFileController {

    private final FileRefManager fileRefManager;

    public DfsFileController(FileRefManager fileRefManager) {
        this.fileRefManager = fileRefManager;
    }

    @PostMapping("/metadata/save-dfs-file")
    public ResponseEntity<String> saveDfsFile(@RequestBody DfsFile dfsFile) {
        try {
            // Get the hash from the FileMeta inside DfsFile
            String hash = dfsFile.getFileMeta().getHash();

            // Call the FileRefManager to save the hash mapping
            fileRefManager.saveHashMapping(hash, dfsFile);

            return ResponseEntity.status(HttpStatus.CREATED)
                    .body("DfsFile with hash " + hash + " has been saved successfully.");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error saving DfsFile: " + e.getMessage());
        } catch (NullPointerException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Invalid DfsFile: " + e.getMessage());
        }
    }

    @PostMapping("/metadata/get-dfs-file")
    public ResponseEntity<?> getDfsFile(@RequestParam String hash) {
        try {
            // Use FileRefManager to read the DfsFile using the provided hash
            Optional<DfsFile> dfsFileOptional = fileRefManager.readHashMapping(hash);

            if (dfsFileOptional.isPresent()) {
                return ResponseEntity.ok(dfsFileOptional.get());
            } else {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body("DfsFile with hash " + hash + " not found.");
            }
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error retrieving DfsFile: " + e.getMessage());
        }
    }
}
