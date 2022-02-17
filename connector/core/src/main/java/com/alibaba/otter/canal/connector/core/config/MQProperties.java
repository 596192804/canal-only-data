package com.alibaba.otter.canal.connector.core.config;

/**
 * MQ配置类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class MQProperties {

    private boolean flatMessage             = true;
    private boolean flatMessageOnlyData     = false;
    private boolean databaseHash            = true;
    private boolean filterTransactionEntry  = true;
    private Integer parallelBuildThreadSize = 8;
    private Integer parallelSendThreadSize  = 30;
    private Integer fetchTimeout            = 100;
    private Integer batchSize               = 50;
    private String  accessChannel           = "local";
//    private String  flatMessageSqlType      = "INSERT";

    private String  aliyunAccessKey         = "";
    private String  aliyunSecretKey         = "";
    private int     aliyunUid               = 0;

    private String  ckURL                   = "";
    private String  ckUsername              = "";
    private String  ckPassword              = "";
    private String  ckClusterName           = "";

    public String getCkClusterName() {
        return ckClusterName;
    }

    public void setCkClusterName(String ckClusterName) {
        this.ckClusterName = ckClusterName;
    }

    public String getCkURL() {
        return ckURL;
    }

    public void setCkURL(String ckURL) {
        this.ckURL = ckURL;
    }

    public String getCkUsername() {
        return ckUsername;
    }

    public void setCkUsername(String ckUsername) {
        this.ckUsername = ckUsername;
    }

    public String getCkPassword() {
        return ckPassword;
    }

    public void setCkPassword(String ckPassword) {
        this.ckPassword = ckPassword;
    }

    public boolean isFlatMessage() {
        return flatMessage;
    }

    public void setFlatMessage(boolean flatMessage) {
        this.flatMessage = flatMessage;
    }

    public boolean isFlatMessageOnlyData() {
        return flatMessageOnlyData;
    }

    public void setFlatMessageOnlyData(boolean flatMessageOnlyData) {
        this.flatMessageOnlyData = flatMessageOnlyData;
    }

//    public void setFlatMessageSqlType(String flatMessageSqlType) {
//        this.flatMessageSqlType = flatMessageSqlType;
//    }
//
//    public String getFlatMessageSqlType() {
//        return this.flatMessageSqlType;
//    }

    public boolean isDatabaseHash() {
        return databaseHash;
    }

    public void setDatabaseHash(boolean databaseHash) {
        this.databaseHash = databaseHash;
    }

    public boolean isFilterTransactionEntry() {
        return filterTransactionEntry;
    }

    public void setFilterTransactionEntry(boolean filterTransactionEntry) {
        this.filterTransactionEntry = filterTransactionEntry;
    }

    public Integer getParallelBuildThreadSize() {
        return parallelBuildThreadSize;
    }

    public void setParallelBuildThreadSize(Integer parallelBuildThreadSize) {
        this.parallelBuildThreadSize = parallelBuildThreadSize;
    }

    public Integer getParallelSendThreadSize() {
        return parallelSendThreadSize;
    }

    public void setParallelSendThreadSize(Integer parallelSendThreadSize) {
        this.parallelSendThreadSize = parallelSendThreadSize;
    }

    public Integer getFetchTimeout() {
        return fetchTimeout;
    }

    public void setFetchTimeout(Integer fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(String accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getAliyunAccessKey() {
        return aliyunAccessKey;
    }

    public void setAliyunAccessKey(String aliyunAccessKey) {
        this.aliyunAccessKey = aliyunAccessKey;
    }

    public String getAliyunSecretKey() {
        return aliyunSecretKey;
    }

    public void setAliyunSecretKey(String aliyunSecretKey) {
        this.aliyunSecretKey = aliyunSecretKey;
    }

    public int getAliyunUid() {
        return aliyunUid;
    }

    public void setAliyunUid(int aliyunUid) {
        this.aliyunUid = aliyunUid;
    }
}
