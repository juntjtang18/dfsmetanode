package com.fdu.msacs.dfs.metanode.mongodb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "registered_node")
public class RegisteredNode {
    @Id
    private String id; // Auto-generated ID
    @Indexed(unique = true)
    private String nodeUrl;
	public String getNodeUrl() {
		return nodeUrl;
	}
	public void setNodeUrl(String nodeUrl) {
		this.nodeUrl = nodeUrl;
	}

    // Getters and Setters
}