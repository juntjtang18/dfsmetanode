package com.infolink.dfs.metanode.mdb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

@Document(collection = "nodeFileMappings")
public class NodeFileMapping {
    @Id
    private String nodeUrl;
    private SortedSet<String> filenames;

    public NodeFileMapping() {
        this.filenames = new TreeSet<>();
    }

    public NodeFileMapping(String nodeUrl) {
        this.nodeUrl = nodeUrl;
        this.filenames = new TreeSet<>();
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

    public void setFilenames(SortedSet<String> filenames) {
        this.filenames = filenames;
    }
}
