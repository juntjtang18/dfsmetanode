package com.fdu.msacs.dfs.metanode.mdb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Document(collection = "block_node")
public class BlockNode {
    @Id
    private String hash;
    private Set<String> nodeUrls = new HashSet<String>();
    
	public String getHash() {
		return hash;
	}
	public void setHash(String hash) {
		this.hash = hash;
	}
	public Set<String> getNodeUrls() {
		return nodeUrls;
	}
	public void setNodeUrls(Set<String> nodeUrls) {
		this.nodeUrls = nodeUrls;
	}

}