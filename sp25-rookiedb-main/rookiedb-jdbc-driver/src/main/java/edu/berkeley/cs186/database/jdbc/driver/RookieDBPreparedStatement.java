package edu.berkeley.cs186.database.jdbc.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class RookieDBPreparedStatement extends RookieDBStatement implements PreparedStatement {
    private static final Logger logger = LoggerFactory.getLogger(RookieDBPreparedStatement.class);

    private final String originalSql;
    private final Map<Integer, Object> parameters;
    private final List<Integer> parameterIndexes;

    public RookieDBPreparedStatement(RookieDBConnection connection, String sql) {
        super(connection);
        this.originalSql = sql;
        this.parameters = new HashMap<>();
        this.parameterIndexes = findParameterIndexes(sql);
        logger.debug("Created PreparedStatement for SQL: {}", sql);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(buildExecutableSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(buildExecutableSql());
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(buildExecutableSql());
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        parameters.clear();
    }

    // 参数设置方法
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    // 批处理支持
    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        super.addBatch(buildExecutableSql());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return new RookieDBParameterMetaData(parameterIndexes.size());
    }

    // 辅助方法
    private List<Integer> findParameterIndexes(String sql) {
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?') {
                indexes.add(indexes.size() + 1); // JDBC 参数索引从 1 开始
            }
        }
        return indexes;
    }

    private void validateParameterIndex(int parameterIndex) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > parameterIndexes.size()) {
            throw new SQLException("Parameter index out of range: " + parameterIndex);
        }
    }

    private String buildExecutableSql() throws SQLException {
        String sql = originalSql;

        // 检查所有参数是否都已设置
        for (int i = 1; i <= parameterIndexes.size(); i++) {
            if (!parameters.containsKey(i)) {
                throw new SQLException("Parameter " + i + " is not set");
            }
        }

        // 替换参数占位符
        StringBuilder result = new StringBuilder();
        int paramIndex = 1;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '?') {
                Object value = parameters.get(paramIndex++);
                result.append(formatParameter(value));
            } else {
                result.append(c);
            }
        }

        return result.toString();
    }

    private String formatParameter(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + value.toString().replace("'", "''") + "'";
        } else if (value instanceof Date || value instanceof Time || value instanceof Timestamp) {
            return "'" + value + "'";
        } else {
            return value.toString();
        }
    }

    // 不支持的方法
    @Override public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setRef(int parameterIndex, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int parameterIndex, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int parameterIndex, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setArray(int parameterIndex, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public ResultSetMetaData getMetaData() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException { setDate(parameterIndex, x); }
    @Override public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException { setTime(parameterIndex, x); }
    @Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException { setTimestamp(parameterIndex, x); }
    @Override public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {throw new SQLFeatureNotSupportedException();}
    @Override public void setURL(int parameterIndex, URL x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setRowId(int parameterIndex, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNString(int parameterIndex, String value) throws SQLException { setString(parameterIndex, value); }
    @Override public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int parameterIndex, NClob value) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int parameterIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int parameterIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int parameterIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
}
