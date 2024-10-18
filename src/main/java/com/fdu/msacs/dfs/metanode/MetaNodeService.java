package com.fdu.msacs.dfs.metanode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fdu.msacs.dfs.metanode.mdb.FileNodeMapping;
import com.fdu.msacs.dfs.metanode.mdb.FileNodeMappingRepo;
import com.fdu.msacs.dfs.metanode.mdb.NodeFileMapping;
import com.fdu.msacs.dfs.metanode.mdb.NodeFileMappingRepo;

@Service
public class MetaNodeService {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeService.class);
    
    @Autowired
    private FileNodeMappingRepo fileNodeMappingRepo;
    @Autowired
    private NodeFileMappingRepo nodeFileMappingRepo;

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

	public boolean checkFileExists(String filename) {
		Optional<FileNodeMapping> fnm = fileNodeMappingRepo.findByFilename(filename);
		return !fnm.isEmpty();
	}
	
}
