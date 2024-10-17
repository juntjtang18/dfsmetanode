package com.fdu.msacs.dfs.metanode;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import com.fdu.msacs.dfs.metanode.mdb.BlockNodeMappingRepo;
import com.fdu.msacs.dfs.metanode.mdb.FileNodeMappingRepo;
import com.fdu.msacs.dfs.metanode.mdb.NodeFileMappingRepo;
import com.fdu.msacs.dfs.metanode.meta.DfsNode;

@SpringBootTest
public class MetaNodeServiceTest {

    @Autowired
    private MetaNodeService metaNodeService;

    @Autowired
    private FileNodeMappingRepo fileNodeMappingRepo;

    @Autowired
    private NodeFileMappingRepo nodeFileMappingRepo;

    @Autowired
    private BlockNodeMappingRepo blockNodeMappingRepo;

    @Autowired
    private NodeManager nodeManager;

    private String testNodeUrl = "http://localhost:8080/node1";
    private String testFilename = "testFile.txt";
    private String testHash = "abc123";

    @BeforeEach
    public void setUp() {
        // Clear the repositories before each test
        metaNodeService.clearCache();
        
        // Optionally register a node for testing
        DfsNode dfsNode = new DfsNode();
        dfsNode.setContainerUrl(testNodeUrl);
        nodeManager.registerNode(dfsNode);
    }

    @Test
    public void testRegisterFileLocation() {
        String result = metaNodeService.registerFileLocation(testFilename, testNodeUrl);
        assertThat(result).isEqualTo("File location registered: " + testFilename + " on " + testNodeUrl);

        // Verify that the filename is registered
        List<String> nodeUrls = metaNodeService.getNodesForFile(testFilename);
        assertThat(nodeUrls).contains(testNodeUrl);
    }

    @Test
    public void testGetNodesForFile() {
        metaNodeService.registerFileLocation(testFilename, testNodeUrl);
        List<String> nodeUrls = metaNodeService.getNodesForFile(testFilename);
        assertThat(nodeUrls).containsExactly(testNodeUrl);
    }

    @Test
    public void testGetNodeFiles() {
        metaNodeService.registerFileLocation(testFilename, testNodeUrl);
        List<String> files = metaNodeService.getNodeFiles(testNodeUrl);
        assertThat(files).containsExactly(testFilename);
    }

    @Test
    public void testSelectNodeForUpload() {
        DfsNode selectedNode = metaNodeService.selectNodeForUpload();
        assertThat(selectedNode).isNotNull();
        assertThat(selectedNode.getContainerUrl()).isEqualTo(testNodeUrl);
    }

    @Test
    public void testClearCache() {
        metaNodeService.registerFileLocation(testFilename, testNodeUrl);
        metaNodeService.clearCache();

        List<String> nodeUrls = metaNodeService.getNodesForFile(testFilename);
        assertThat(nodeUrls).isEmpty(); // Cache should be cleared
    }
}
