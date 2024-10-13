package com.fdu.msacs.dfs.metanode;

import java.io.File;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;

@Configuration
public class AppConfig {

    @Value("${spring.data.mongodb.uri}")
    private String mongodbUri;

    @PostConstruct
    public void init() {
        String effectiveUri = EnvironmentUtils.isInDocker() 
            ? "mongodb://mongodb:27017" 
            : "mongodb://localhost:27017";

        System.setProperty("spring.data.mongodb.uri", effectiveUri);
    }
    
    @Bean
    public MongoDatabaseFactory mongoDatabaseFactory() {
        String mongodbUri = System.getProperty("spring.data.mongodb.uri");
        String databaseName = "dfsdb"; // Replace with your database name
        return new SimpleMongoClientDatabaseFactory(mongodbUri + "/" + databaseName);
    }

    @Bean
    public MongoTemplate mongoTemplate() {
        return new MongoTemplate(mongoDatabaseFactory());
    }
    
    @Bean
	public RestTemplate restTemplate() {
		return new RestTemplate();
	}
	
	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}
	
    public static boolean isInDocker() {
        return new File("/.dockerenv").exists() || System.getenv("DOCKER_CONTAINER") != null;
    }
}
