package com.infolink.dfs.metanode;

public class RequestReplicationNodes {
    private String filename;
    private String requestingNodeUrl;

    public RequestReplicationNodes() {
    	this.filename = "";
    	this.requestingNodeUrl = "";
    }
    public RequestReplicationNodes(String filename, String containerUrl) {
		this.filename = filename;
		this.requestingNodeUrl = containerUrl;
	}

	// Getters and Setters
    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getRequestingNodeUrl() {
        return requestingNodeUrl;
    }

    public void setRequestingNodeUrl(String requestingNodeUrl) {
        this.requestingNodeUrl = requestingNodeUrl;
    }
}