package edu.berkeley.cs186.database.jdbc.driver;

import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class RookieDBResultSetMetaData implements ResultSetMetaData {
    private final Schema schema;

    public RookieDBResultSetMetaData(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return schema.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false; // RookieDB doesn't support auto-increment
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true; // String comparisons are case-sensitive
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true; // All columns are searchable
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false; // No currency type in RookieDB
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable; // Assume all columns are nullable
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        Type type = getColumnTypeType(column);
        return type == Type.intType() || type == Type.longType() || type == Type.floatType();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        Type type = getColumnTypeType(column);
        if (type == Type.intType()) {
            return 11; // -2147483648
        } else if (type == Type.longType()) {
            return 20; // -9223372036854775808
        } else if (type == Type.floatType()) {
            return 15; // Approximate display size for float
        } else if (type == Type.boolType()) {
            return 5; // "false"
        }
//        else if (type.isFixedSizeType()) {
//            return type.getSizeInBytes();
//        }
        else {
            return 255; // Default for variable-length types
        }
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        validateColumnIndex(column);
        return schema.getFieldName(column - 1); // JDBC is 1-indexed
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return ""; // RookieDB doesn't have schema concept
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        Type type = getColumnTypeType(column);
        if (type == Type.intType()) {
            return 10; // 10 digits
        } else if (type == Type.longType()) {
            return 19; // 19 digits
        } else if (type == Type.floatType()) {
            return 7; // Approximate precision for float
        }
//        else if (type.isFixedSizeType()) {
//            return type.getSizeInBytes();
//        }
        else {
            return 0; // Variable length
        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        Type type = getColumnTypeType(column);
        if (type == Type.floatType()) {
            return 7; // Approximate scale for float
        }
        return 0; // No decimal places for other types
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return ""; // Table name not available in schema
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return ""; // RookieDB doesn't have catalog concept
    }


    @Override
    public String getColumnTypeName(int column) throws SQLException {
        Type type = getColumnTypeType(column);
        return TypeConverter.rookieTypeToSqlTypeName(type);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true; // ResultSet is read-only
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false; // ResultSet is read-only
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false; // ResultSet is read-only
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        Type type = getColumnTypeType(column);
        return TypeConverter.rookieTypeToJavaClassName(type);
    }

    private Type getColumnTypeType(int column) throws SQLException {
        validateColumnIndex(column);
        return schema.getFieldType(column - 1); // JDBC is 1-indexed
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        Type type = getColumnTypeType(column);
        return TypeConverter.rookieTypeToSqlType(type);
    }

    private void validateColumnIndex(int column) throws SQLException {
        if (column < 1 || column > schema.size()) {
            throw new RookieDBSQLException("Invalid column index: " + column, "07009");
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new RookieDBSQLException("Cannot unwrap to " + iface.getName(), "22023");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

}
