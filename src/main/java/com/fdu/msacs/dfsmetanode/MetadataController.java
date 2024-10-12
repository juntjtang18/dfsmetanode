package com.fdu.msacs.dfsmetanode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet; // Import HashSet
import java.util.List;
import java.util.Set; // Import Set

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriUtils;

import com.fdu.msacs.dfsmetanode.mongodb.FileNodes;
import com.fdu.msacs.dfsmetanode.mongodb.FileNodeMappingRepository;
import com.fdu.msacs.dfsmetanode.mongodb.NodeFiles;
import com.fdu.msacs.dfsmetanode.mongodb.NodeFileMappingRepository;
import com.fdu.msacs.dfsmetanode.mongodb.RegisteredNode;
import com.fdu.msacs.dfsmetanode.mongodb.RegisteredNodeRepository;

@RestController
public class MetadataController {
    private static final Logger logger = LoggerFactory.getLogger(MetadataController.class);

    private final FileNodeMappingRepository fileNodeMapping;
    private final NodeFileMappingRepository nodeFileMapping;
    private final RegisteredNodeRepository registeredNodeRepository;
    private Set<String> registeredNodes = new HashSet<>();

    public MetadataController(FileNodeMappingRepository fileNodeRepository, NodeFileMappingRepository nodeFileRepository, RegisteredNodeRepository registeredNodeRepository) {
        this.fileNodeMapping = fileNodeRepository;
        this.nodeFileMapping = nodeFileRepository;
        this.registeredNodeRepository = registeredNodeRepository;
    }

    @PostMapping("/metadata/register-node")
    public ResponseEntity<String> registerNode(@RequestBody RequestNode request) {
        String nodeAddress = request.getNodeUrl();

        // Attempt to register the node
        if (registeredNodeRepository.findByNodeUrl(nodeAddress) == null) {
            RegisteredNode registeredNode = new RegisteredNode();
            registeredNode.setNodeUrl(nodeAddress);
            registeredNodeRepository.save(registeredNode);
            registeredNodes.add(nodeAddress); // Update local cache
            logger.info("A new node registered: {}", nodeAddress);
            return ResponseEntity.ok("Node registered: " + nodeAddress);
        } else {
            logger.warn("Node already registered: {}", nodeAddress);
            return ResponseEntity.status(409).body("Node already registered: " + nodeAddress); // Conflict status
        }
    }

    @PostMapping("/metadata/register-file-location")
    public ResponseEntity<String> registerFileLocation(@RequestBody RequestFileLocation request) {
        String filename = request.getFilename();
        String nodeUrl = request.getNodeUrl();

        // Update or create the FileNode entry
        FileNodes fileNode = fileNodeMapping.findByFilename(filename);
        if (fileNode == null) {
            fileNode = new FileNodes();
            fileNode.setFilename(filename);
            fileNode.setNodeUrls(new ArrayList<>());
        }
        fileNode.getNodeUrls().add(nodeUrl);
        fileNodeMapping.save(fileNode);

        // Update or create the NodeFileMapping entry
        NodeFiles nodeFiles = nodeFileMapping.findByNodeUrl(nodeUrl);
        if (nodeFiles == null) {
            nodeFiles = new NodeFiles();
            nodeFiles.setNodeUrl(nodeUrl);
            nodeFiles.setFilenames(new ArrayList<>());
        }
        nodeFiles.getFilenames().add(filename);
        nodeFileMapping.save(nodeFiles);
        
        logger.info("File {} registered to : {}", filename, nodeUrl);
        logger.info("Current fileNodeMap: {}", fileNodeMapping.findAll());
        logger.info("Current nodeFileMapping: {}", nodeFileMapping.findAll());

        return ResponseEntity.ok("File location registered: " + filename + " on " + nodeUrl);
    }

    @GetMapping("/metadata/nodes-for-file/{filename}")
    public ResponseEntity<List<String>> getNodesForFile(@PathVariable String filename) {
        FileNodes fileNode = fileNodeMapping.findByFilename(filename);
        List<String> nodeAddresses = fileNode != null ? fileNode.getNodeUrls() : new ArrayList<>();
        logger.info("Searching the node for file {}, return {}", filename, nodeAddresses);
        return ResponseEntity.ok(nodeAddresses);
    }

    @PostMapping("/metadata/get-replication-nodes")
    public ResponseEntity<List<String>> getReplicationNodes(@RequestBody RequestReplicationNodes request) {
        String filename = request.getFilename();
        String requestingNodeUrl = request.getRequestingNodeUrl();

        logger.info("/metadata/get-replication-nodes called for filename: {} and requestingNodeUrl: {}", filename, requestingNodeUrl);
        String decodedRequestingNodeUrl = UriUtils.decode(requestingNodeUrl, StandardCharsets.UTF_8);

        List<String> availableNodes = new ArrayList<>(registeredNodes);
        availableNodes.remove(decodedRequestingNodeUrl);

        FileNodes fileNode = fileNodeMapping.findByFilename(filename);
        if (fileNode != null) {
            availableNodes.removeAll(fileNode.getNodeUrls());
        }

        logger.info("Available nodes after filtering: {}", availableNodes);
        return ResponseEntity.ok(availableNodes);
    }

    @GetMapping("/metadata/get-registered-nodes")
    public ResponseEntity<Set<String>> getRegisteredNodes() {
        registeredNodes.clear();
        registeredNodes.addAll(registeredNodeRepository.findAll().stream()
                .map(RegisteredNode::getNodeUrl)
                .toList());
        return ResponseEntity.ok(registeredNodes);
    }

    @PostMapping("/metadata/get-node-files")
    public ResponseEntity<List<String>> getNodeFiles(@RequestBody RequestNode request) {
    	List<String> files = new ArrayList<String>();
    	NodeFiles nodeFiles = nodeFileMapping.findByNodeUrl(request.getNodeUrl());
        if (nodeFiles!=null) {
        	files.addAll(nodeFiles.getFilenames());
        }
         
        return ResponseEntity.ok(files);
    }

    @GetMapping("/metadata/get-file-node-mapping/{filename}")
    public ResponseEntity<List<String>> getFileNodeMapping(@PathVariable String filename) {
        FileNodes fileNode = fileNodeMapping.findByFilename(filename);
        List<String> nodeList = fileNode != null ? fileNode.getNodeUrls() : new ArrayList<>();
        logger.info("nodes {} has the file {}", nodeList, filename);
        return ResponseEntity.ok(nodeList);
    }

    @PostMapping("/metadata/clear-cache")
    public ResponseEntity<String> clearCache() {
        fileNodeMapping.deleteAll(); // Clear all file-node mappings
        nodeFileMapping.deleteAll();
        logger.info("Cache cleared.");
        return ResponseEntity.ok("Cache cleared");
    }

    @PostMapping("/metadata/clear-registered-nodes")
    public ResponseEntity<String> clearRegisteredNodes() {
        registeredNodeRepository.deleteAll(); // Clear all registered nodes
        registeredNodes.clear();
        logger.info("Registered nodes cleared.");
        return ResponseEntity.ok("Registered nodes cleared.");
    }

    @GetMapping("/metadata/pingsvr")
    public String pingSvr() {
        return "Metadata Server is running...";
    }
}
