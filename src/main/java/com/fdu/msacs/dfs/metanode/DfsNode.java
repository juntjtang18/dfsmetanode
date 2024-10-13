package com.fdu.msacs.dfs.metanode;

public class DfsNode {
	private String nodeUrl;
	private String hostPort;
	
	public DfsNode() {
		this.nodeUrl = "";
		this.hostPort = "";
	}
	
	public DfsNode(String nodeUrl) {
		this.nodeUrl = nodeUrl;
		this.hostPort = "";
	}
	
	public DfsNode(String nodeUrl, String hostPort) {
		this.nodeUrl = nodeUrl;
		this.hostPort = hostPort;
	}
	
	public String getNodeUrl() {
		return nodeUrl;
	}
	
	public void setNodeUrl(String nodeUrl) {
		this.nodeUrl = nodeUrl;
	}
	
	public String getHostPort() {
		return hostPort;
	}
	
	public void setHostPort(String hostPort) {
		this.hostPort = hostPort;
	}
}
