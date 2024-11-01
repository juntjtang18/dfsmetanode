package com.infolink.dfs.metanode;

import java.util.List;

import com.infolink.dfs.shared.DfsNode;

public class RefreshNodeEvent {
    private List<DfsNode> registeredNodes;

    // Constructor, getters, and setters
    public RefreshNodeEvent(List<DfsNode> registeredNodes) {
        this.registeredNodes = registeredNodes;
    }

    public List<DfsNode> getRegisteredNodes() {
        return registeredNodes;
    }
}