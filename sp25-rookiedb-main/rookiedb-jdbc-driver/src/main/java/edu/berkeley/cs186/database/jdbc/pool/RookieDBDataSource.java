package edu.berkeley.cs186.database.jdbc.pool;

import edu.berkeley.cs186.database.jdbc.driver.RookieDBDriver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class RookieDBDataSource implements DataSource, AutoCloseable{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RookieDBDataSource.class);

    private BasicDataSource dataSource;
    private RookieDBPoolConfiguration config;


    public RookieDBDataSource(String jdbcUrl, RookieDBPoolConfiguration config) {
        this.config = config;
        this.config.validate(); // 验证配置
        this.dataSource = config.createBasicDataSource(jdbcUrl);
        logger.info("RookieDB DataSource initialized with URL: {}", jdbcUrl);
    }


    public RookieDBDataSource(String jdbcUrl) {
        this(jdbcUrl, new RookieDBPoolConfiguration());
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            logger.info("Closing RookieDB DataSource");
            dataSource.close();
            dataSource = null;
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (dataSource == null) {
            throw new SQLException("DataSource has been closed");
        }
        return dataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
