package com.infolink.dfs.metanode;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import com.infolink.dfs.shared.DfsNode;

@Controller
public class MetaNodeController {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeController.class);
	
	@Autowired
	private NodeManager nodeManager;
	
    @GetMapping("/")
    public String homePage(Model model) {  // Add Model parameter
        List<DfsNode> registeredNodes = nodeManager.getRegisteredNodes();
        logger.debug("registeredNodes={}", registeredNodes);
        
        model.addAttribute("registeredNodes", registeredNodes);  // Add the registeredNodes to the model
        model.addAttribute("currentPage", "home");
        
        return "index"; // returns index.html
    }
    
    @GetMapping("/files")
    public String dfsFiles(Model model) {
        model.addAttribute("currentPage", "files");
    	logger.debug("/files requested.");
    	return "files";
    }
}
