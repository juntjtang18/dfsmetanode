package com.fdu.msacs.dfs.metanode;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fdu.msacs.dfs.metanode.mdb.BlockNode;
import com.fdu.msacs.dfs.metanode.mdb.BlockNodeMappingRepo;
import com.fdu.msacs.dfs.metanode.mdb.FileNodeMapping;
import com.fdu.msacs.dfs.metanode.mdb.FileNodeMappingRepo;
import com.fdu.msacs.dfs.metanode.mdb.NodeFileMapping;
import com.fdu.msacs.dfs.metanode.mdb.NodeFileMappingRepo;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;
import com.fdu.msacs.dfs.metanode.meta.FileRefManager;

@Service
public class MetaNodeService {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeService.class);
    
    @Autowired
    private FileNodeMappingRepo fileNodeMappingRepo;
    @Autowired
    private NodeFileMappingRepo nodeFileMappingRepo;
    @Autowired
    private BlockNodeMappingRepo blockNodeMappingRepo;
    @Autowired
    private FileRefManager fileRefManager;
    @Autowired
    private NodeManager nodeManager;  // New NodeManager class for node management logic

    public String registerFileLocation(String filename, String nodeUrl) {
        // Update or create the FileNode entry
        FileNodeMapping fileNodeMapping = fileNodeMappingRepo.findByFilename(filename)
                .orElse(new FileNodeMapping(filename));
        fileNodeMapping.getNodeUrls().add(nodeUrl);
        fileNodeMappingRepo.save(fileNodeMapping);

        // Update or create the NodeFile entry
        NodeFileMapping nodeFileMapping = nodeFileMappingRepo.findByNodeUrl(nodeUrl)
                .orElse(new NodeFileMapping(nodeUrl));
        nodeFileMapping.getFilenames().add(filename);
        nodeFileMappingRepo.save(nodeFileMapping);

        logger.info("File {} registered to : {}", filename, nodeUrl);
        return "File location registered: " + filename + " on " + nodeUrl;
    }

    public String registerBlockLocation(String hash, String nodeUrl) {
        logger.debug("MetaService: registerBlockLocation: {}->{}", hash, nodeUrl);

        BlockNode blockNode = blockNodeMappingRepo.findByHash(hash);
        if (blockNode == null) {
            blockNode = new BlockNode();
            blockNode.setHash(hash);
            blockNode.getNodeUrls().add(nodeUrl);
        }
        blockNodeMappingRepo.save(blockNode);
        logger.debug("Block {} registered to : {}", hash, nodeUrl);
        logger.debug("Current blockNodeMapping: {}", blockNodeMappingRepo.findAll());
        return "Block location registered: " + hash + " on " + nodeUrl;
    }

    public List<String> getNodesForFile(String filename) {
        FileNodeMapping fileNodeMapping = fileNodeMappingRepo.findByFilename(filename).orElse(new FileNodeMapping());
        List<String> nodeUrls = new ArrayList<>(fileNodeMapping.getNodeUrls());
        logger.info("Searching the node for file {}, return {}", filename, nodeUrls);
        return nodeUrls;
    }

    public List<String> getNodeFiles(String nodeUrl) {
        NodeFileMapping nodeFileMapping = nodeFileMappingRepo.findByNodeUrl(nodeUrl).orElse(new NodeFileMapping());
        return new ArrayList<>(nodeFileMapping.getFilenames());
    }

    public void clearCache() {
        fileNodeMappingRepo.deleteAll();
        nodeFileMappingRepo.deleteAll();
        logger.info("Cache cleared.");
    }

    public List<String> getFileNodeMapping(String filename) {
        FileNodeMapping fnmap = fileNodeMappingRepo.findByFilename(filename).orElse(new FileNodeMapping());
        return new ArrayList<>(fnmap.getNodeUrls());
    }

    public DfsNode selectNodeForUpload() {
        return nodeManager.selectNodeForUpload();  // Delegate node selection to NodeManager
    }
}
