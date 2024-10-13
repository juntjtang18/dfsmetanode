package com.fdu.msacs.dfs.metanode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class MetaNodeServiceTest2 {
	@Autowired
    private MetaNodeService metaNodeService;

    @BeforeEach
    public void setUp() {
        metaNodeService.clearCache();
        metaNodeService.clearRegisteredNodes();
    }

    @Test
    public void testRegisterBlockLocation() {
        DfsNode node = new DfsNode("http://localhost:8081", "http://lcalhost:8081");
        metaNodeService.registerNode(node);
        
        String response = metaNodeService.registerBlockLocation("blockhash-abcd1234", node.getContainerUrl());
        assertEquals("Block location registered: blockhash-abcd1234 on http://localhost:8081", response);
        
        response = metaNodeService.registerBlockLocation("blockhash-abcd1234", node.getContainerUrl());
        assertEquals("Block location registered: blockhash-abcd1234 on http://localhost:8081", response);
    }

}
