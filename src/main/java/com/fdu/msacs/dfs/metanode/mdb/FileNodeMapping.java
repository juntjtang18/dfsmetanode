package com.fdu.msacs.dfs.metanode.mdb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashSet;
import java.util.Set;

@Document(collection = "fileNodeMappings")
public class FileNodeMapping {
    @Id
    private String filename;
    private Set<String> nodeUrls;

    public FileNodeMapping() {
        this.nodeUrls = new HashSet<>();
    }

    public FileNodeMapping(String filename) {
        this.filename = filename;
        this.nodeUrls = new HashSet<>();
    }

    // Getters and Setters
    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Set<String> getNodeUrls() {
        return nodeUrls;
    }

    public void setNodeUrls(Set<String> nodeUrls) {
        this.nodeUrls = nodeUrls;
    }
}
