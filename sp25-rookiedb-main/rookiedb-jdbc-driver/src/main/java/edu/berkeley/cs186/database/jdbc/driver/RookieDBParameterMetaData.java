package edu.berkeley.cs186.database.jdbc.driver;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class RookieDBParameterMetaData implements ParameterMetaData {
    private final int parameterCount;

    public RookieDBParameterMetaData(int parameterCount) {
        this.parameterCount = parameterCount;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return true; // 假设所有数值类型都是有符号的
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0; // 未知精度
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0; // 未知标度
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return Types.OTHER; // 未知类型
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return "UNKNOWN";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return Object.class.getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
