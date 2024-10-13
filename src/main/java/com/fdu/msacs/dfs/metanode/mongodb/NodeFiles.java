package com.fdu.msacs.dfs.metanode.mongodb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.util.List;

@Document(collection = "node_file_mappings") // Specify the collection name
public class NodeFiles {
    @Id
    private String id; // MongoDB document ID
    private String nodeUrl;
    private List<String> filenames; // List of filenames associated with this node

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public void setNodeUrl(String nodeUrl) {
        this.nodeUrl = nodeUrl;
    }

    public List<String> getFilenames() {
        return filenames;
    }

    public void setFilenames(List<String> filenames) {
        this.filenames = filenames;
    }
}
