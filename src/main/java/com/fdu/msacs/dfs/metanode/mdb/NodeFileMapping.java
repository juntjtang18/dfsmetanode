package com.fdu.msacs.dfs.metanode.mdb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashSet;
import java.util.Set;

@Document(collection = "nodeFileMappings")
public class NodeFileMapping {
    @Id
    private String nodeUrl;
    private Set<String> filenames;

    public NodeFileMapping() {
        this.filenames = new HashSet<>();
    }

    public NodeFileMapping(String nodeUrl) {
        this.nodeUrl = nodeUrl;
        this.filenames = new HashSet<>();
    }

    // Getters and Setters
    public String getNodeUrl() {
        return nodeUrl;
    }

    public void setNodeUrl(String nodeUrl) {
        this.nodeUrl = nodeUrl;
    }

    public Set<String> getFilenames() {
        return filenames;
    }

    public void setFilenames(Set<String> filenames) {
        this.filenames = filenames;
    }
}
