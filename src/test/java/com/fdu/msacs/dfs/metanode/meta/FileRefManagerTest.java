package com.fdu.msacs.dfs.metanode.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FileRefManagerTest {

    private FileRefManager fileRefManager;
    private ObjectMapper objectMapper;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        fileRefManager = new FileRefManager(tempDir.toString());  // Use tempDir for testing
    }

    @Test
    void testSaveHashMapping() throws IOException {
        String hash = "abc123456789defabcd5678abcdef12";
        Set<String> blocks = Set.of("block1", "block2", "block3");
        FileMeta fileMeta = new FileMeta("d:\\mybackupfiles\\a\\f.txt", 12345, "2024-10-06T12:34:56Z");
        BackupFile backupFile = new BackupFile(fileMeta, blocks);

        // Save hash mapping
        String returnedHash = fileRefManager.saveHashMapping(hash, backupFile);

        // Verify the correct hash is returned
        assertEquals(hash, returnedHash);

        // Manually calculate the expected file path
        Path expectedPath = tempDir.resolve("refs").resolve("ab").resolve("c1").resolve("23").resolve("45").resolve("6789defabcd5678abcdef12.json");

        // Check that the file was created at the expected path
        assertTrue(Files.exists(expectedPath));

        // Verify that the contents of the file are correct
        BackupFile savedBackupFile = objectMapper.readValue(expectedPath.toFile(), BackupFile.class);
        assertEquals(backupFile, savedBackupFile);
    }

    @Test
    void testCheckHashMapping() throws IOException {
        String hash = "abc123456789defabcd5678abcdef12";
        Set<String> blocks = Set.of("block1", "block2", "block3");
        FileMeta fileMeta = new FileMeta("d:\\mybackupfiles\\a\\f.txt", 12345, "2024-10-06T12:34:56Z");
        BackupFile backupFile = new BackupFile(fileMeta, blocks);

        // Save the hash mapping first
        fileRefManager.saveHashMapping(hash, backupFile);

        // Check if the hash mapping exists
        Optional<BackupFile> result = fileRefManager.readHashMapping(hash);
        assertTrue(result.isPresent());

        // Verify the retrieved content
        BackupFile retrievedBackupFile = result.get();
        assertEquals(backupFile, retrievedBackupFile);
    }

    @Test
    void testFileHashExists() throws IOException {
        String hash = "abc123456789defabcd5678abcdef12";
        Set<String> blocks = Set.of("block1", "block2", "block3");
        FileMeta fileMeta = new FileMeta("d:\\mybackupfiles\\a\\f.txt", 12345, "2024-10-06T12:34:56Z");
        BackupFile backupFile = new BackupFile(fileMeta, blocks);

        // Save the hash mapping first
        fileRefManager.saveHashMapping(hash, backupFile);

        // Check if the hash file exists
        assertTrue(fileRefManager.fileHashExists(hash));
    }

    @Test
    void testFileHashDoesNotExist() throws IOException {
        String nonExistentHash = "nonexistent1234567890abcdefabcdef12";

        // Check if the non-existent hash file does not exist
        assertFalse(fileRefManager.fileHashExists(nonExistentHash));
    }
}
