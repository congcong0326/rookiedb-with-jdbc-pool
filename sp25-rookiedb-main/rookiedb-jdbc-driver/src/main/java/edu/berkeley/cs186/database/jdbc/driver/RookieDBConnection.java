package edu.berkeley.cs186.database.jdbc.driver;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class RookieDBConnection implements Connection {
    private static final Logger logger = LoggerFactory.getLogger(RookieDBConnection.class);

    private final Database database;
    private final ConnectionInfo connectionInfo;
    private Transaction currentTransaction;
    private boolean closed = false;
    private boolean autoCommit = true;
    private int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;

    public RookieDBConnection(Database database, ConnectionInfo connectionInfo) {
        this.database = database;
        this.connectionInfo = connectionInfo;
        logger.debug("Created RookieDBConnection: {}", connectionInfo);
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new RookieDBStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new RookieDBPreparedStatement(this, sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.autoCommit != autoCommit) {
            if (!autoCommit && currentTransaction == null) {
                // 开始新事务
                currentTransaction = database.beginTransaction();
                logger.debug("Started new transaction: {}", currentTransaction.getTransNum());
            } else if (autoCommit && currentTransaction != null) {
                // 提交当前事务
                currentTransaction.commit();
                currentTransaction = null;
                logger.debug("Committed transaction due to autoCommit change");
            }
            this.autoCommit = autoCommit;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot commit when autoCommit is enabled");
        }
        if (currentTransaction != null) {
            currentTransaction.commit();
            currentTransaction = database.beginTransaction(); // 开始新事务
            logger.debug("Committed transaction and started new one");
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot rollback when autoCommit is enabled");
        }
        if (currentTransaction != null) {
            currentTransaction.rollback();
            currentTransaction = database.beginTransaction(); // 开始新事务
            logger.debug("Rolled back transaction and started new one");
        }
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            try {
                if (currentTransaction != null) {
                    if (autoCommit) {
                        currentTransaction.commit();
                    } else {
                        currentTransaction.rollback();
                    }
                    currentTransaction = null;
                }
                //database.close();

                logger.debug("Closed RookieDB connection");
            } catch (Exception e) {
                logger.error("Error closing connection", e);
                throw new SQLException("Error closing connection: " + e.getMessage(), e);
            } finally {
                closed = true;
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    // 获取当前事务（内部使用）
    public Transaction getCurrentTransaction() throws SQLException {
        checkClosed();
        if (currentTransaction == null && !autoCommit) {
            currentTransaction = database.beginTransaction();
        }
        return currentTransaction;
    }

    // 获取数据库实例（内部使用）
    public Database getDatabase() {
        return database;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }

    // 以下是其他必需的 Connection 接口方法的基础实现
    // 为了简洁，这里只提供基本框架

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new RookieDBDatabaseMetaData(this);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        // RookieDB 目前只支持基本的隔离级别
        if (level != Connection.TRANSACTION_READ_COMMITTED &&
                level != Connection.TRANSACTION_SERIALIZABLE) {
            throw new SQLException("Unsupported transaction isolation level: " + level);
        }
        this.transactionIsolation = level;
    }

    // 添加获取 URL 的方法
    public String getURL() {
        if (connectionInfo == null) {
            return null;
        }

        StringBuilder url = new StringBuilder("jdbc:rookiedb:");

        // 如果有主机信息，构建网络 URL
        if (connectionInfo.getHost() != null && !connectionInfo.getHost().isEmpty()) {
            url.append("//")
                    .append(connectionInfo.getHost())
                    .append(":")
                    .append(connectionInfo.getPort());
        }

        // 添加数据库路径
        if (connectionInfo.getDatabasePath() != null) {
            if (!connectionInfo.getDatabasePath().startsWith("/")) {
                url.append("/");
            }
            url.append(connectionInfo.getDatabasePath());
        }

        return url.toString();
    }

    // 其他方法的默认实现...
    @Override public CallableStatement prepareCall(String sql) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String nativeSQL(String sql) throws SQLException { return sql; }
    @Override public void setReadOnly(boolean readOnly) throws SQLException { /* 忽略 */ }
    @Override public boolean isReadOnly() throws SQLException { return false; }
    @Override public void setCatalog(String catalog) throws SQLException { /* 忽略 */ }
    @Override public String getCatalog() throws SQLException { return null; }
    @Override public SQLWarning getWarnings() throws SQLException { return null; }
    @Override public void clearWarnings() throws SQLException { /* 忽略 */ }
    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException { return createStatement(); }
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { return prepareStatement(sql); }
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Map<String, Class<?>> getTypeMap() throws SQLException { return null; }
    @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException { /* 忽略 */ }
    @Override public void setHoldability(int holdability) throws SQLException { /* 忽略 */ }
    @Override public int getHoldability() throws SQLException { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public Savepoint setSavepoint() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Savepoint setSavepoint(String name) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void rollback(Savepoint savepoint) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return createStatement(); }
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return prepareStatement(sql); }
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException { return prepareStatement(sql); }
    @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException { return prepareStatement(sql); }
    @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException { return prepareStatement(sql); }
    @Override public Clob createClob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob createBlob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob createNClob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML createSQLXML() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean isValid(int timeout) throws SQLException { return !closed; }
    @Override public void setClientInfo(String name, String value) throws SQLClientInfoException { /* 忽略 */ }
    @Override public void setClientInfo(Properties properties) throws SQLClientInfoException { /* 忽略 */ }
    @Override public String getClientInfo(String name) throws SQLException { return null; }
    @Override public Properties getClientInfo() throws SQLException { return new Properties(); }
    @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setSchema(String schema) throws SQLException { /* 忽略 */ }
    @Override public String getSchema() throws SQLException { return null; }
    @Override public void abort(Executor executor) throws SQLException { close(); }
    @Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException { /* 忽略 */ }
    @Override public int getNetworkTimeout() throws SQLException { return 0; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("Not a wrapper"); }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }
}
