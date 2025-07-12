package edu.berkeley.cs186.database.jdbc.pool;

import edu.berkeley.cs186.database.jdbc.driver.RookieDBDriver;
import org.apache.commons.dbcp2.BasicDataSource;

public class RookieDBPoolConfiguration {
    // 连接池基础配置
    private int maxTotal = 20;
    private int maxIdle = 20;
    private int minIdle = 0;
    private long maxWaitMillis = -1;

    // RookieDB特定配置
    private int bufferSize = 51200;
    private boolean enableLocking = true;
    private boolean enableRecovery = true;
    private int workMem = 1024;
    
    // 验证配置
    public void validate() throws IllegalArgumentException {
        if (maxTotal <= 0) {
            throw new IllegalArgumentException("maxTotal must be positive");
        }
        if (maxIdle < 0) {
            throw new IllegalArgumentException("maxIdle cannot be negative");
        }
        if (minIdle < 0) {
            throw new IllegalArgumentException("minIdle cannot be negative");
        }
        if (minIdle > maxIdle) {
            throw new IllegalArgumentException("minIdle cannot be greater than maxIdle");
        }
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be positive");
        }
        if (workMem <= 0) {
            throw new IllegalArgumentException("workMem must be positive");
        }
    }
    
    // 转换为DBCP2配置
    public BasicDataSource createBasicDataSource(String jdbcUrl) {
        BasicDataSource ds = new BasicDataSource();

        // 基础连接配置
        ds.setUrl(jdbcUrl);
        ds.setDriverClassName(RookieDBDriver.class.getName());
        // 连接池配置
        ds.setMaxTotal(maxTotal);
        ds.setMaxIdle(maxIdle);
        ds.setMinIdle(minIdle);
        ds.setMaxWaitMillis(maxWaitMillis);

        // 连接验证配置
        // 连接验证配置 - 嵌入式数据库无需验证查询
        ds.setTestOnBorrow(false);
        ds.setTestWhileIdle(false);
        // 移除验证查询：ds.setValidationQuery("SELECT 1");

        // RookieDB 特定属性
        ds.addConnectionProperty("bufferSize", String.valueOf(bufferSize));
        ds.addConnectionProperty("enableLocking", String.valueOf(enableLocking));
        ds.addConnectionProperty("enableRecovery", String.valueOf(enableRecovery));
        ds.addConnectionProperty("workMem", String.valueOf(workMem));

        return ds;
    }
}