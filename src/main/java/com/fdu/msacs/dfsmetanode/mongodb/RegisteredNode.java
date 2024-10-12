package com.fdu.msacs.dfsmetanode.mongodb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "registered_node")
public class RegisteredNode {
    @Id
    private String id; // Auto-generated ID
    private String nodeUrl;
	public String getNodeUrl() {
		return nodeUrl;
	}
	public void setNodeUrl(String nodeUrl) {
		this.nodeUrl = nodeUrl;
	}

    // Getters and Setters
}