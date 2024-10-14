package com.fdu.msacs.dfs.metanode;

import com.fdu.msacs.dfs.metanode.meta.DfsNode;
import com.fdu.msacs.dfs.metanode.meta.DfsNode.HealthStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.scheduling.annotation.EnableScheduling;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@EnableScheduling
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class MetaNodeServiceScheduledTest {
    private static final Logger logger = LoggerFactory.getLogger(MetaNodeServiceScheduledTest.class);

    @Autowired
    private MetaNodeService metaNodeService;

    private int HEALTH_CHECK_THRESHOLD = 6; // seconds
    private int WARNING_THRESHOLD = 3; // seconds

    @BeforeEach
    public void setUp() {
        // Set thresholds directly
        metaNodeService.HEALTH_CHECK_THRESHOLD = HEALTH_CHECK_THRESHOLD;
        metaNodeService.WARNING_THRESHOLD = WARNING_THRESHOLD;

        // Register nodes with different last report times
        Date now = new Date(); // Get the current time as a Date object
        DfsNode healthyNode = new DfsNode("node1", now); // Healthy node, reports now
        DfsNode warningNode = new DfsNode("node2", new Date(now.getTime() - (WARNING_THRESHOLD + 1) * 1000)); // Warning node
        DfsNode downNode = new DfsNode("node3", new Date(now.getTime() - (HEALTH_CHECK_THRESHOLD + 1) * 1000)); // Down node

        logger.info("Registering healthyNode with last report time: {}", healthyNode.getLastTimeReport());
        logger.info("Registering warningNode with last report time: {}", warningNode.getLastTimeReport());
        logger.info("Registering downNode with last report time: {}", downNode.getLastTimeReport());

        metaNodeService.registerNode(healthyNode);
        metaNodeService.registerNode(warningNode);
        metaNodeService.registerNode(downNode);
    }

    @Test
    public void testCheckNodeHealth() {
        // Step 1: Register multiple nodes with different health statuses.
        List<DfsNode> nodes = metaNodeService.getRegisteredNodes();
        logger.info("Registered nodes before health check: {}", nodes);

        // Step 2: Perform the health check.
        metaNodeService.checkNodeHealth();
        
        // Step 3: Verify that the nodes' statuses are updated correctly.
        List<DfsNode> registeredNodes = metaNodeService.getRegisteredNodes();
        List<DfsNode> nodesAfterHealthCheck = metaNodeService.getRegisteredNodes();
        logger.info("Registered nodes after health check: {}", nodesAfterHealthCheck);

        // Step 4: Validate the node statuses after the health check.
        DfsNode updatedHealthyNode = registeredNodes.stream()
                .filter(node -> node.getContainerUrl().equals("node1"))
                .findFirst()
                .orElse(null);

        DfsNode updatedWarningNode = registeredNodes.stream()
                .filter(node -> node.getContainerUrl().equals("node2"))
                .findFirst()
                .orElse(null);

        DfsNode updatedDownNode = registeredNodes.stream()
                .filter(node -> node.getContainerUrl().equals("node3"))
                .findFirst()
                .orElse(null);

        assertEquals(HealthStatus.HEALTHY, updatedHealthyNode.getHealthStatus());
        assertEquals(HealthStatus.WARNING, updatedWarningNode.getHealthStatus());
        assertEquals(null, updatedDownNode, "The down node should be removed from the registered nodes.");
    }

    //@Test
    public void testCheckNodeHealthScheduledExecution() throws InterruptedException {
        // Use a CountDownLatch to wait for the scheduled method to run
        CountDownLatch latch = new CountDownLatch(1);

        // Wait for a few seconds to ensure the @Scheduled method is triggered
        latch.await(5, TimeUnit.SECONDS);

        // Verify the results after the scheduled execution
        List<DfsNode> registeredNodes = metaNodeService.getRegisteredNodes();

        // Validate the node statuses after the health check
        DfsNode updatedHealthyNode = registeredNodes.stream()
                .filter(node -> node.getContainerUrl().equals("node1"))
                .findFirst()
                .orElse(null);

        DfsNode updatedWarningNode = registeredNodes.stream()
                .filter(node -> node.getContainerUrl().equals("node2"))
                .findFirst()
                .orElse(null);

        DfsNode updatedDownNode = registeredNodes.stream()
                .filter(node -> node.getContainerUrl().equals("node3"))
                .findFirst()
                .orElse(null);

        assertEquals(HealthStatus.HEALTHY, updatedHealthyNode.getHealthStatus());
        assertEquals(HealthStatus.WARNING, updatedWarningNode.getHealthStatus());
        assertEquals(null, updatedDownNode, "The down node should be removed from the registered nodes.");
    }

}
