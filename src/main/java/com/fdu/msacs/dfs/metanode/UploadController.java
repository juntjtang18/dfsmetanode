package com.fdu.msacs.dfs.metanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UploadController {
    private static Logger logger = LoggerFactory.getLogger(MetaNodeController.class);
    @Autowired
    private MetaNodeService metaNodeService;
    @Autowired
    private NodeManager nodeManager; // Reference to the NodeManager for node management
    

}
