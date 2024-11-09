package com.infolink.dfs.metanode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import jakarta.annotation.PostConstruct;

@SpringBootApplication
@ComponentScan(basePackages = {"com.infolink.dfs.metanode", "com.infolink.dfs.metanode.mdb", "com.infolink.dfs.metanode.event"})
//@EnableMongoRepositories(basePackages = "com.infolink.dfs.metanode.mdb")
public class DfsmetasvrApplication {

    public static void main(String[] args) {
        SpringApplication.run(DfsmetasvrApplication.class, args);
    }
    
}
