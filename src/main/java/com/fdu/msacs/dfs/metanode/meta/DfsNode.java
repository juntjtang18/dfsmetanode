package com.fdu.msacs.dfs.metanode.meta;

import java.util.Date;

public class DfsNode {
    private String containerUrl;
    private String localUrl;
    private Date lastTimeReport; // Last time the health status was reported
    private HealthStatus healthStatus; // Health status

    public DfsNode() {
        this.containerUrl = "";
        this.localUrl = "";
        this.lastTimeReport = new Date(); // Initialize to current time
        this.healthStatus = HealthStatus.HEALTHY; // Initial status
    }
    
    public DfsNode(String containerUrl, String localUrl) {
        this.containerUrl = containerUrl;
        this.localUrl = localUrl;
        this.lastTimeReport = new Date(); // Initialize to current time
        this.healthStatus = HealthStatus.HEALTHY; // Initial status
    }

    public DfsNode(String containerUrl) {
        this.containerUrl = containerUrl;
        this.localUrl = containerUrl;
        this.lastTimeReport = new Date(); // Initialize to current time
        this.healthStatus = HealthStatus.HEALTHY; // Initial status
    }

    public DfsNode(String containerUrl, Date lastTimeReport) {
        this.containerUrl = containerUrl;
        this.localUrl = containerUrl;
        this.lastTimeReport = lastTimeReport; // Use provided time
        this.healthStatus = HealthStatus.HEALTHY; // Initial status
    }

    public String getContainerUrl() {
        return containerUrl;
    }

    public void setContainerUrl(String containerUrl) {
        this.containerUrl = containerUrl;
    }

    public String getLocalUrl() {
        return localUrl;
    }

    public void setLocalUrl(String localUrl) {
        this.localUrl = localUrl;
    }

    @Override
    public String toString() {
        return "DfsNode{" +
                "containerUrl='" + containerUrl + '\'' +
                ", localUrl='" + localUrl + '\'' +
                ", lastTimeReport=" + lastTimeReport +
                ", healthStatus=" + healthStatus +
                '}';
    }

    public Date getLastTimeReport() {
        return lastTimeReport;
    }

    public void setLastTimeReport(Date lastTimeReport) {
        this.lastTimeReport = lastTimeReport;
    }

    public HealthStatus getHealthStatus() {
        return healthStatus;
    }

    public void setHealthStatus(HealthStatus healthStatus) {
        this.healthStatus = healthStatus;
        this.lastTimeReport = new Date(); // Update report time when status changes
    }
    
    public void updateHealthStatus(HealthStatus healthStatus) {
        setHealthStatus(healthStatus);
    }

    public enum HealthStatus {
        HEALTHY,
        WARNING,
        DEAD
    }
}
