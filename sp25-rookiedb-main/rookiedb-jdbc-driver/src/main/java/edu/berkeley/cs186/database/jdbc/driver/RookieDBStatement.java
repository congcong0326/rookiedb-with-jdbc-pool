package edu.berkeley.cs186.database.jdbc.driver;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import edu.berkeley.cs186.database.table.Record;

public class RookieDBStatement implements Statement {
    private static final Logger logger = LoggerFactory.getLogger(RookieDBStatement.class);

    protected final RookieDBConnection connection;
    protected boolean closed = false;
    protected int maxRows = 0;
    protected int queryTimeout = 0;
    protected int fetchSize = 0;
    protected ResultSet currentResultSet;
    protected int updateCount = -1;
    protected List<String> batchCommands;

    public RookieDBStatement(RookieDBConnection connection) {
        this.connection = connection;
        this.batchCommands = new ArrayList<>();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        logger.debug("Executing query: {}", sql);

        Transaction transaction = null;
        try {
            transaction = getTransaction();

            // 执行查询
            var queryPlanOpt = transaction.execute(sql);
            if (queryPlanOpt.isPresent()) {
                QueryPlan queryPlan = queryPlanOpt.get();

                // 从 QueryPlan 中获取记录和模式
                List<Record> records = new ArrayList<>();
                Iterator<Record> iterator = queryPlan.execute();
                while (iterator.hasNext()) {
                    Record next = iterator.next();
                    records.add(next);
                }
                Schema schema = queryPlan.getFinalOperator().getSchema();

                // 正确的构造函数调用
                currentResultSet = new RookieDBResultSet(records, schema, this);
                updateCount = -1;

                // 在自动提交模式下关闭事务
                closeTransactionIfAutoCommit(transaction);

                return currentResultSet;
            } else {
                throw new SQLException("Query did not return a result set: " + sql);
            }
        } catch (Exception e) {
            // 发生异常时回滚事务（如果是自动提交模式）
            if (connection.getAutoCommit() && transaction != null) {
                try {
                    transaction.rollback();
                } catch (Exception rollbackEx) {
                    logger.error("Error rolling back transaction after query failure", rollbackEx);
                }
            }
            logger.error("Error executing query: {}", sql, e);
            throw new SQLException("Error executing query: " + e.getMessage(), e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        logger.debug("Executing update: {}", sql);

        Transaction transaction = null;
        try {
            transaction = getTransaction();

            // 执行更新语句
            var result = transaction.execute(sql);

            // 对于 DDL 和 DML 语句，通常不返回 QueryPlan
            if (result.isEmpty()) {
                // 假设操作成功，返回受影响的行数
                updateCount = parseUpdateCount(sql);
                currentResultSet = null;

                // 在自动提交模式下关闭事务
                closeTransactionIfAutoCommit(transaction);

                return updateCount;
            } else {
                throw new SQLException("Update statement returned a result set: " + sql);
            }
        } catch (Exception e) {
            // 发生异常时回滚事务（如果是自动提交模式）
            if (connection.getAutoCommit() && transaction != null) {
                try {
                    transaction.rollback();
                } catch (Exception rollbackEx) {
                    logger.error("Error rolling back transaction after update failure", rollbackEx);
                }
            }
            logger.error("Error executing update: {}", sql, e);
            throw new SQLException("Error executing update: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        logger.debug("Executing statement: {}", sql);

        Transaction transaction = null;
        try {
            transaction = getTransaction();
            var result = transaction.execute(sql);

            if (result.isPresent()) {
                // 返回了 QueryPlan，说明是查询语句
                QueryPlan queryPlan = result.get();

                // 从 QueryPlan 中获取记录和模式
                List<Record> records = new ArrayList<>();
                Iterator<Record> iterator = queryPlan.execute();
                while (iterator.hasNext()) {
                    records.add(iterator.next());
                }
                Schema schema = queryPlan.getFinalOperator().getSchema();

                // 正确的构造函数调用
                currentResultSet = new RookieDBResultSet(records, schema, this);
                updateCount = -1;

                // 在自动提交模式下关闭事务
                closeTransactionIfAutoCommit(transaction);

                return true;
            } else {
                // 没有返回 QueryPlan，说明是更新语句
                updateCount = parseUpdateCount(sql);
                currentResultSet = null;

                // 在自动提交模式下关闭事务
                closeTransactionIfAutoCommit(transaction);

                return false;
            }
        } catch (Exception e) {
            // 发生异常时回滚事务（如果是自动提交模式）
            if (connection.getAutoCommit() && transaction != null) {
                try {
                    transaction.rollback();
                } catch (Exception rollbackEx) {
                    logger.error("Error rolling back transaction after execute failure", rollbackEx);
                }
            }
            logger.error("Error executing statement: {}", sql, e);
            throw new SQLException("Error executing statement: " + e.getMessage(), e);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        // RookieDB 不支持多结果集
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
        }
        updateCount = -1;
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            try {
                if (currentResultSet != null) {
                    currentResultSet.close();
                    currentResultSet = null;
                }
                logger.debug("Closed RookieDBStatement");
            } finally {
                closed = true;
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    // 批处理支持
    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batchCommands.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batchCommands.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();

        int[] results = new int[batchCommands.size()];

        try {
            for (int i = 0; i < batchCommands.size(); i++) {
                String sql = batchCommands.get(i);
                try {
                    results[i] = executeUpdate(sql);
                } catch (SQLException e) {
                    // 批处理失败
                    throw new BatchUpdateException("Batch execution failed at command " + i, results);
                }
            }
            return results;
        } finally {
            clearBatch();
        }
    }

    // 辅助方法
    protected Transaction getTransaction() throws SQLException {
        if (connection.getAutoCommit()) {
            // 自动提交模式下，每个语句都在独立的事务中执行
            return connection.getDatabase().beginTransaction();
        } else {
            // 手动事务模式
            return connection.getCurrentTransaction();
        }
    }

    /**
     * 根据自动提交模式决定是否关闭事务
     */
    protected void closeTransactionIfAutoCommit(Transaction transaction) throws SQLException {
        if (connection.getAutoCommit() && transaction != null) {
            try {
                transaction.commit();
            } catch (Exception e) {
                logger.error("Error committing auto-commit transaction", e);
                try {
                    transaction.rollback();
                } catch (Exception rollbackEx) {
                    logger.error("Error rolling back transaction", rollbackEx);
                }
                throw new SQLException("Error managing auto-commit transaction: " + e.getMessage(), e);
            }
        }
    }

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
        if (connection.isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    /**
     * 解析更新语句影响的行数
     * 这是一个简化的实现，实际中需要根据具体的执行结果来确定
     */
    private int parseUpdateCount(String sql) {
        String upperSql = sql.trim().toUpperCase();
        if (upperSql.startsWith("CREATE") || upperSql.startsWith("DROP") ||
                upperSql.startsWith("ALTER")) {
            return 0; // DDL 语句
        }
        return 1; // 假设 DML 语句影响了 1 行，实际实现需要从执行结果中获取
    }



    // 其他 Statement 接口方法的基础实现
    @Override public int getMaxFieldSize() throws SQLException { return 0; }
    @Override public void setMaxFieldSize(int max) throws SQLException { /* 忽略 */ }
    @Override public int getMaxRows() throws SQLException { return maxRows; }
    @Override public void setMaxRows(int max) throws SQLException { this.maxRows = max; }
    @Override public void setEscapeProcessing(boolean enable) throws SQLException { /* 忽略 */ }
    @Override public int getQueryTimeout() throws SQLException { return queryTimeout; }
    @Override public void setQueryTimeout(int seconds) throws SQLException { this.queryTimeout = seconds; }
    @Override public void cancel() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLWarning getWarnings() throws SQLException { return null; }
    @Override public void clearWarnings() throws SQLException { /* 忽略 */ }
    @Override public void setCursorName(String name) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setFetchDirection(int direction) throws SQLException { /* 忽略 */ }
    @Override public int getFetchDirection() throws SQLException { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchSize(int rows) throws SQLException { this.fetchSize = rows; }
    @Override public int getFetchSize() throws SQLException { return fetchSize; }
    @Override public int getResultSetConcurrency() throws SQLException { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int getResultSetType() throws SQLException { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public Connection getConnection() throws SQLException { return connection; }
    @Override public boolean getMoreResults(int current) throws SQLException { return getMoreResults(); }
    @Override public ResultSet getGeneratedKeys() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException { return executeUpdate(sql); }
    @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException { return executeUpdate(sql); }
    @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException { return executeUpdate(sql); }
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, String[] columnNames) throws SQLException { return execute(sql); }
    @Override public int getResultSetHoldability() throws SQLException { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public boolean isPoolable() throws SQLException { return false; }
    @Override public void setPoolable(boolean poolable) throws SQLException { /* 忽略 */ }
    @Override public void closeOnCompletion() throws SQLException { /* 忽略 */ }
    @Override public boolean isCloseOnCompletion() throws SQLException { return false; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("Not a wrapper"); }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }
}
