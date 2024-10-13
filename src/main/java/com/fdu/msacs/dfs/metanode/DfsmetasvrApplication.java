package com.fdu.msacs.dfs.metanode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.fdu.msacs.dfs.metanode", "com.fdu.msacs.dfs.metanode.mongodb"})
public class DfsmetasvrApplication {

	public static void main(String[] args) {
		SpringApplication.run(DfsmetasvrApplication.class, args);
	}

}
