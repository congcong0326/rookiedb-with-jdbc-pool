package edu.berkeley.cs186.database.jdbc.driver;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Schema;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import edu.berkeley.cs186.database.table.Record;

public class RookieDBResultSet implements ResultSet {

    private final List<Record> records;
    private final Schema schema;
    private final Statement statement;
    private int currentRow = -1;
    private boolean closed = false;
    private boolean wasNull = false;
    private final RookieDBResultSetMetaData metaData;

    public RookieDBResultSet(List<Record> records, Schema schema, Statement statement) {
        this.records = records != null ? records : new ArrayList<>();
        this.schema = schema;
        this.statement = statement;
        this.metaData = new RookieDBResultSetMetaData(schema);
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new RookieDBSQLException("ResultSet is closed", "24000");
        }
    }

    private void checkValidRow() throws SQLException {
        checkClosed();
        if (currentRow < 0 || currentRow >= records.size()) {
            throw new RookieDBSQLException("Invalid cursor position", "24000");
        }
    }

    private DataBox getDataBox(int columnIndex) throws SQLException {
        checkValidRow();
        if (columnIndex < 1 || columnIndex > schema.size()) {
            throw new RookieDBSQLException("Invalid column index: " + columnIndex, "07009");
        }

        Record record = records.get(currentRow);
        DataBox dataBox = record.getValue(columnIndex - 1); // JDBC is 1-indexed
        wasNull = (dataBox == null);
        return dataBox;
    }

    private DataBox getDataBox(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDataBox(columnIndex);
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        currentRow++;
        return currentRow < records.size();
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToString(dataBox);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToBoolean(dataBox);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToByte(dataBox);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToShort(dataBox);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToInt(dataBox);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToLong(dataBox);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToFloat(dataBox);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToDouble(dataBox);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("not support BigDecimal");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToBytes(dataBox);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToDate(dataBox);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToTime(dataBox);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToTimestamp(dataBox);
    }

    // String-based getters
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("not support BigDecimal");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return metaData;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        for (int i = 0; i < schema.size(); i++) {
            if (schema.getFieldName(i).equalsIgnoreCase(columnLabel)) {
                return i + 1; // JDBC is 1-indexed
            }
        }
        throw new RookieDBSQLException("Column not found: " + columnLabel, "42S22");
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        DataBox dataBox = getDataBox(columnIndex);
        return TypeConverter.dataBoxToObject(dataBox);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRow >= 0 && currentRow < records.size() ? currentRow + 1 : 0;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return currentRow < 0 && !records.isEmpty();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return currentRow >= records.size() && !records.isEmpty();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return currentRow == 0 && !records.isEmpty();
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return currentRow == records.size() - 1 && !records.isEmpty();
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        currentRow = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        currentRow = records.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (records.isEmpty()) {
            return false;
        }
        currentRow = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        if (records.isEmpty()) {
            return false;
        }
        currentRow = records.size() - 1;
        return true;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        if (row == 0) {
            beforeFirst();
            return false;
        }

        if (row > 0) {
            currentRow = row - 1; // JDBC is 1-indexed
        } else {
            currentRow = records.size() + row; // negative indexing from end
        }

        return currentRow >= 0 && currentRow < records.size();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        return absolute(getRow() + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        currentRow--;
        return currentRow >= 0;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new RookieDBSQLException("Only FETCH_FORWARD is supported", "0A000");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0; // Use driver default
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // Ignore for now
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    // Unsupported operations
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("getAsciiStream not supported", "0A000");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("getUnicodeStream not supported", "0A000");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("getBinaryStream not supported", "0A000");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("getAsciiStream not supported", "0A000");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("getUnicodeStream not supported", "0A000");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("getBinaryStream not supported", "0A000");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // No warnings to clear
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new RookieDBSQLException("Named cursors not supported", "0A000");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("getCharacterStream not supported", "0A000");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("getCharacterStream not supported", "0A000");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("not support BigDecimal");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("not support BigDecimal");
    }

    // Update operations - not supported
    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    // String-based update methods
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("Ref not supported", "0A000");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("Blob not supported", "0A000");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("Clob not supported", "0A000");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("Array not supported", "0A000");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("Ref not supported", "0A000");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("Blob not supported", "0A000");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("Clob not supported", "0A000");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("Array not supported", "0A000");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("URL not supported", "0A000");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("URL not supported", "0A000");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("RowId not supported", "0A000");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("RowId not supported", "0A000");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public int getHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("NClob not supported", "0A000");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("NClob not supported", "0A000");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("SQLXML not supported", "0A000");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("SQLXML not supported", "0A000");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new RookieDBSQLException("getNCharacterStream not supported", "0A000");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new RookieDBSQLException("getNCharacterStream not supported", "0A000");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new RookieDBSQLException("Updates not supported", "0A000");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object obj = getObject(columnIndex);
        return TypeConverter.convertToType(obj, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
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
