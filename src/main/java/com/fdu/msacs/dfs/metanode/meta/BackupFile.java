package com.fdu.msacs.dfs.metanode.meta;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

public class BackupFile implements Serializable {
    private static final long serialVersionUID = 1L; // Unique identifier for serialization
    private FileMeta fileMeta;                // File metadata
    private Set<String> blockHashes;     // Mapping of block hashes to block data

    public BackupFile() {
    	this.fileMeta = null;
    	this.blockHashes = null;
    }
    
    // Constructor
    public BackupFile(FileMeta fileMeta, Set<String> blockMap) {
        this.fileMeta = fileMeta;
        this.blockHashes = blockMap;
    }

    // Getters
    public FileMeta getFileMeta() {
        return fileMeta;
    }

    public Set<String> getBlockHashes() {
        return blockHashes;
    }
    
    public void setFileMeta(FileMeta fileMeta) {
    	this.fileMeta = fileMeta;
    }
    
    public void setBlockHashes(Set<String> hashes) {
    	this.blockHashes = hashes;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupFile that = (BackupFile) o;
        return Objects.equals(fileMeta, that.fileMeta) && Objects.equals(blockHashes, that.blockHashes);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fileMeta, blockHashes);
    }

    // Other methods for backup operations (e.g., addBlock, removeBlock, etc.)
}
