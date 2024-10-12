package com.fdu.msacs.dfsmetanode.mongodb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Document(collection = "file_node")
public class FileNodes {
    @Id
    private String id; // Auto-generated ID
    private String filename;
    private List<String> nodeUrls;
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public List<String> getNodeUrls() {
		return nodeUrls;
	}
	public void setNodeUrls(List<String> nodeUrls) {
		this.nodeUrls = nodeUrls;
	}

    // Getters and Setters
}
