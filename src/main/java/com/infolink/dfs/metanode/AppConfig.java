package com.infolink.dfs.metanode;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.infolink.dfs.shared.DfsFile;

import jakarta.annotation.PostConstruct;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
@EnableMongoRepositories(basePackages = "com.fdu.msacs.dfs.metanode.mdb")

public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    
    @Value("${spring.data.mongodb.uri}")
    private String mongodbUri;
    @Value("${dfs.replication.factor:3}")
    private int replicateFactor;
    @Value("${spring.redis.host}")
    private String redisHost;
    
    @PostConstruct
    public void init() {
        // Set MongoDB URI dynamically based on environment
        String effectiveMongoUri = EnvironmentUtils.isInDocker() 
            ? "mongodb://mongodb:27017" 
            : "mongodb://localhost:27017";
        System.setProperty("spring.data.mongodb.uri", effectiveMongoUri);
        
        // Set Redis host dynamically based on environment
        String effectiveRedisHost = EnvironmentUtils.isInDocker() 
            ? "redis" 
            : "localhost";
        System.setProperty("spring.redis.host", effectiveRedisHost);
        
        logger.info("MongoDB URI set to: {}", effectiveMongoUri);
        logger.info("Redis host set to: {}", effectiveRedisHost);
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
    public RedisConnectionFactory redisConnectionFactory() {
        return new LettuceConnectionFactory();
    }
    
    @Bean
    public RedisTemplate<String, DfsFile> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, DfsFile> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        // Use Jackson for value serialization
        Jackson2JsonRedisSerializer<DfsFile> valueSerializer = new Jackson2JsonRedisSerializer<>(DfsFile.class);
        template.setValueSerializer(valueSerializer);
        
        // Use StringRedisSerializer for keys
        template.setKeySerializer(new StringRedisSerializer());
        
        return template;
    }

    
	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}
	
    public static boolean isInDocker() {
        return new File("/.dockerenv").exists() || System.getenv("DOCKER_CONTAINER") != null;
    }
    
    
    public int getReplicationFactor() {
    	return replicateFactor;
    }
    
    public static String getAppDir() {
        try {
            // Get the path of the running class or JAR
            String jarPath = AppConfig.class.getProtectionDomain().getCodeSource().getLocation().toString();
            logger.info("jarPath: {}", jarPath);

            File jarFile;

            // If the path is a URI (starts with "jar:file:" or "file:")
            if (jarPath.startsWith("jar:file:")) {
                jarPath = jarPath.substring(9, jarPath.indexOf('!')); // Extract the JAR path
                jarFile = new File(new URI(jarPath)); // Handle as URI
            } else if (jarPath.startsWith("file:")) {
                jarPath = jarPath.substring(5); // Strip "file:" prefix and handle as file path
                jarFile = new File(jarPath);
            } else {
                // Handle as a regular file path (no URI involved)
                jarFile = new File(jarPath);
            }

            // Call helper method to handle the app directory logic
            return handleAppDirectory(jarFile);

        } catch (URISyntaxException | IllegalArgumentException e) {
            logger.error("Failed to get app directory", e);
            throw new RuntimeException("Failed to get app directory", e);
        }
    }

    private static String handleAppDirectory(File jarFile) {
        // Get the parent directory of the JAR file
        File jarDir = jarFile.getParentFile();

        // Check if we're running from class files in Eclipse (./target/classes)
        if (jarDir != null && jarDir.getName().equals("classes")) {
            // Navigate up two levels to reach ./target
            jarDir = jarDir.getParentFile(); // Go to target
        }

        // Default to a fallback directory if jarDir is null or doesn't exist (e.g., in a container)
        if (jarDir == null || !jarDir.exists()) {
            logger.warn("Jar directory is null or does not exist: {}", jarDir);
            jarDir = new File("/app"); // Fallback to /app in a container environment
        }

        // Create the "dfs" directory within the jarDir
        File dfsDir = new File(jarDir, "dfs");

        // Create the directory if it doesn't exist
        if (!dfsDir.exists()) {
            boolean created = dfsDir.mkdirs();
            if (!created) {
                logger.error("Failed to create directory: {}", dfsDir.getAbsolutePath());
            } else {
                logger.info("Directory created successfully: {}", dfsDir.getAbsolutePath());
            }
        } else {
            logger.info("Directory already exists: {}", dfsDir.getAbsolutePath());
        }

        return dfsDir.getAbsolutePath();
    } 
    
}
