package com.infolink.dfs.metanode.event;

import com.infolink.dfs.shared.DfsNode;

public class DeadNodeEvent {
    private final DfsNode deadNode;

    public DeadNodeEvent(DfsNode deadNode) {
        this.deadNode = deadNode;
    }

    public DfsNode getDeadNode() {
        return deadNode;
    }
}
