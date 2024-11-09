package com.infolink.dfs.metanode;

import com.infolink.dfs.shared.DfsNode;

public class RequestReplicateBlock {
    private String blockHash;
    private DfsNode targetNode;

    // Getters and Setters
    public String getBlockHash() {
        return blockHash;
    }

    public void setBlockHash(String blockHash) {
        this.blockHash = blockHash;
    }

    public DfsNode getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(DfsNode targetNode) {
        this.targetNode = targetNode;
    }
}
