package edu.berkeley.cs186.database.jdbc.driver;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.concurrency.DummyLockManager;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RookieDB JDBC 驱动实现
 * 处理 JDBC URL 格式：jdbc:rookiedb://[host]:[port]/[database_path][?properties]
 */
public class RookieDBDriver implements Driver {
    private static final Logger logger = LoggerFactory.getLogger(RookieDBDriver.class);

    private static final Map<String, Database> databaseInstances = new ConcurrentHashMap<>();

    // JDBC URL 前缀
    private static final String URL_PREFIX = "jdbc:rookiedb:";

    // 驱动版本信息
    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;

    // 静态初始化块 - 自动注册驱动
    static {
        try {
            DriverManager.registerDriver(new RookieDBDriver());
            logger.info("RookieDB JDBC Driver registered successfully");
        } catch (SQLException e) {
            logger.error("Failed to register RookieDB JDBC Driver", e);
            throw new RuntimeException("Failed to register RookieDB JDBC Driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // 检查 URL 是否被此驱动支持
        if (!acceptsURL(url)) {
            return null; // 根据 JDBC 规范，不支持的 URL 应返回 null
        }

        try {
            // 解析 JDBC URL
            ConnectionInfo connInfo = parseURL(url, info);

            String dbPath = connInfo.getDatabasePath();

            // 获取或创建数据库实例（单例）
            Database database = getOrCreateDatabase(dbPath, connInfo);

            // 创建并返回连接
            RookieDBConnection connection = new RookieDBConnection(database, connInfo);
            logger.debug("Successfully created connection to: {}", connInfo.getDatabasePath());

            return connection;

        } catch (Exception e) {
            logger.error("Failed to create connection for URL: {}", url, e);
            throw new SQLException("Failed to create connection: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            return false;
        }
        return url.toLowerCase().startsWith(URL_PREFIX.toLowerCase());
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // 定义支持的连接属性
        DriverPropertyInfo[] properties = new DriverPropertyInfo[4];

        // 数据库路径
        properties[0] = new DriverPropertyInfo("databasePath",
                info.getProperty("databasePath", ""));
        properties[0].description = "Path to the database directory";
        properties[0].required = true;

        // 缓冲区大小
        properties[1] = new DriverPropertyInfo("bufferSize",
                info.getProperty("bufferSize", "262144"));
        properties[1].description = "Number of pages in buffer cache (default: 262144)";
        properties[1].required = false;

        // 是否启用锁管理
        properties[2] = new DriverPropertyInfo("enableLocking",
                info.getProperty("enableLocking", "false"));
        properties[2].description = "Enable concurrency control (default: false)";
        properties[2].required = false;
        properties[2].choices = new String[]{"true", "false"};

        // 是否启用恢复管理
        properties[3] = new DriverPropertyInfo("enableRecovery",
                info.getProperty("enableRecovery", "false"));
        properties[3].description = "Enable ARIES recovery manager (default: false)";
        properties[3].required = false;
        properties[3].choices = new String[]{"true", "false"};

        return properties;
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        // RookieDB 是教学数据库，不完全符合 JDBC 规范
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("RookieDB does not support java.util.logging");
    }

    /**
     * 解析 JDBC URL
     * 支持格式：
     * - jdbc:rookiedb:file:///path/to/database
     * - jdbc:rookiedb://localhost:8080/path/to/database
     * - jdbc:rookiedb:///path/to/database
     */
    private ConnectionInfo parseURL(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("Invalid URL format: " + url);
        }

        // 移除前缀
        String remaining = url.substring(URL_PREFIX.length());

        ConnectionInfo connInfo = new ConnectionInfo();

        // 解析不同的 URL 格式
        if (remaining.startsWith("file://")) {
            // 文件协议：jdbc:rookiedb:file:///path/to/database
            connInfo.setDatabasePath(remaining.substring(7)); // 移除 "file://"
        } else if (remaining.startsWith("//")) {
            // 网络协议：jdbc:rookiedb://host:port/path
            parseNetworkURL(remaining.substring(2), connInfo);
        } else if (remaining.startsWith("/")) {
            // 简化格式：jdbc:rookiedb:/path/to/database
            connInfo.setDatabasePath(remaining);
        } else {
            // 直接路径：jdbc:rookiedb:path/to/database
            connInfo.setDatabasePath(remaining);
        }

        // 应用连接属性
        applyProperties(connInfo, info);

        // 验证必需的属性
        if (connInfo.getDatabasePath() == null || connInfo.getDatabasePath().trim().isEmpty()) {
            throw new SQLException("Database path is required");
        }

        return connInfo;
    }

    /**
     * 解析网络格式的 URL
     */
    private void parseNetworkURL(String remaining, ConnectionInfo connInfo) throws SQLException {
        // 目前 RookieDB 是嵌入式数据库，不支持网络连接
        // 但我们可以解析格式以备将来扩展
        int slashIndex = remaining.indexOf('/');
        if (slashIndex == -1) {
            throw new SQLException("Invalid network URL format, missing database path");
        }

        String hostPort = remaining.substring(0, slashIndex);
        String path = remaining.substring(slashIndex);

        // 解析主机和端口
        int colonIndex = hostPort.lastIndexOf(':');
        if (colonIndex != -1) {
            connInfo.setHost(hostPort.substring(0, colonIndex));
            try {
                connInfo.setPort(Integer.parseInt(hostPort.substring(colonIndex + 1)));
            } catch (NumberFormatException e) {
                throw new SQLException("Invalid port number: " + hostPort.substring(colonIndex + 1));
            }
        } else {
            connInfo.setHost(hostPort);
            connInfo.setPort(8080); // 默认端口
        }

        connInfo.setDatabasePath(path);

        // 目前抛出异常，因为不支持网络连接
        throw new SQLException("Network connections are not supported in this version of RookieDB");
    }

    /**
     * 应用连接属性
     */
    private void applyProperties(ConnectionInfo connInfo, Properties info) {
        if (info != null) {
            // 缓冲区大小
            String bufferSize = info.getProperty("bufferSize");
            if (bufferSize != null) {
                try {
                    connInfo.setBufferSize(Integer.parseInt(bufferSize));
                } catch (NumberFormatException e) {
                    logger.warn("Invalid bufferSize value: {}, using default", bufferSize);
                }
            }

            // 锁管理
            String enableLocking = info.getProperty("enableLocking");
            if (enableLocking != null) {
                connInfo.setEnableLocking(Boolean.parseBoolean(enableLocking));
            }

            // 恢复管理
            String enableRecovery = info.getProperty("enableRecovery");
            if (enableRecovery != null) {
                connInfo.setEnableRecovery(Boolean.parseBoolean(enableRecovery));
            }

            // 工作内存
            String workMem = info.getProperty("workMem");
            if (workMem != null) {
                try {
                    connInfo.setWorkMem(Integer.parseInt(workMem));
                } catch (NumberFormatException e) {
                    logger.warn("Invalid workMem value: {}, using default", workMem);
                }
            }
        }
    }

    /**
     * 创建数据库实例
     */
    private Database createDatabase(ConnectionInfo connInfo) throws SQLException {
        try {
            boolean enableLocking = connInfo.isEnableLocking();
            boolean enableRecovery = connInfo.isEnableRecovery();
            Database database = new Database(connInfo.getDatabasePath(), connInfo.getBufferSize(),
                    enableLocking ? new LockManager() : new DummyLockManager(),
                    new ClockEvictionPolicy(),
                    enableRecovery);
            logger.info("Created database instance for path: {}", connInfo.getDatabasePath());
            return database;
        } catch (Exception e) {
            throw new SQLException("Failed to create database instance: " + e.getMessage(), e);
        }
    }

    private synchronized Database getOrCreateDatabase(String dbPath, ConnectionInfo connInfo) throws SQLException {
        Database database = databaseInstances.get(dbPath);
        if (database == null) {
            database = createDatabase(connInfo);
            // 确保Database完全初始化后再放入缓存
            // 可以添加一个简单的验证，比如尝试获取一个事务
            try {
                database.beginTransaction().commit();
            } catch (Exception e) {
                database.close();
                throw new SQLException("Database initialization failed", e);
            }
            databaseInstances.put(dbPath, database);
        }
        return database;
    }


}