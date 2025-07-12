package edu.berkeley.cs186.database.jdbc.driver;

import java.sql.SQLException;

public class RookieDBSQLException extends SQLException {

    // SQL 状态码常量
    public static final String SYNTAX_ERROR = "42000";
    public static final String TABLE_NOT_FOUND = "42S02";
    public static final String COLUMN_NOT_FOUND = "42S22";
    public static final String CONNECTION_FAILURE = "08001";
    public static final String TRANSACTION_ROLLBACK = "40001";
    public static final String FEATURE_NOT_SUPPORTED = "0A000";

    public RookieDBSQLException(String message) {
        super(message);
    }

    public RookieDBSQLException(String message, String sqlState) {
        super(message, sqlState);
    }

    public RookieDBSQLException(String message, String sqlState, int vendorCode) {
        super(message, sqlState, vendorCode);
    }

    public RookieDBSQLException(String message, Throwable cause) {
        super(message, cause);
    }

    public RookieDBSQLException(String message, String sqlState, Throwable cause) {
        super(message, sqlState, cause);
    }

    public RookieDBSQLException(String message, String sqlState, int vendorCode, Throwable cause) {
        super(message, sqlState, vendorCode, cause);
    }

    /**
     * 创建语法错误异常
     */
    public static RookieDBSQLException syntaxError(String message) {
        return new RookieDBSQLException(message, SYNTAX_ERROR);
    }

    /**
     * 创建表不存在异常
     */
    public static RookieDBSQLException tableNotFound(String tableName) {
        return new RookieDBSQLException("Table '" + tableName + "' doesn't exist", TABLE_NOT_FOUND);
    }

    /**
     * 创建列不存在异常
     */
    public static RookieDBSQLException columnNotFound(String columnName) {
        return new RookieDBSQLException("Column '" + columnName + "' not found", COLUMN_NOT_FOUND);
    }

    /**
     * 创建连接失败异常
     */
    public static RookieDBSQLException connectionFailure(String message, Throwable cause) {
        return new RookieDBSQLException(message, CONNECTION_FAILURE, cause);
    }

    /**
     * 创建功能不支持异常
     */
    public static RookieDBSQLException featureNotSupported(String feature) {
        return new RookieDBSQLException("Feature not supported: " + feature, FEATURE_NOT_SUPPORTED);
    }
}
