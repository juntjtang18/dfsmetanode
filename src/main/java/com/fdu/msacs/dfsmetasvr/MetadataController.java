package com.fdu.msacs.dfsmetasvr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/metadata")
public class MetadataController {

    private final Map<String, List<String>> fileNodeMap = new HashMap<>();
    private final List<String> nodes = new ArrayList<>();

    // Register a storage node
    @PostMapping("/register-node")
    public ResponseEntity<String> registerNode(@RequestBody String nodeAddress) {
        nodes.add(nodeAddress);
        return ResponseEntity.ok("Node registered: " + nodeAddress);
    }

    // Get nodes for a specific file
    @GetMapping("/nodes-for-file/{filename}")
    public ResponseEntity<List<String>> getNodesForFile(@PathVariable String filename) {
        List<String> nodeAddresses = fileNodeMap.get(filename);
        if (nodeAddresses == null || nodeAddresses.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        return ResponseEntity.ok(nodeAddresses);
    }

    // Add file to node mapping
    public void addFileToNodes(String filename, List<String> nodes) {
        fileNodeMap.put(filename, nodes);
    }

    // Get all registered nodes
    public List<String> getRegisteredNodes() {
        return nodes;
    }
}
