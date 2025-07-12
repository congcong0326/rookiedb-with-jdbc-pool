package edu.berkeley.cs186.database.jdbc.driver;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class RookieDBDatabaseMetaData implements DatabaseMetaData {

    private final RookieDBConnection connection;
    private final String databaseProductName = "RookieDB";
    private final String databaseProductVersion = "1.0";
    private final String driverName = "RookieDB JDBC Driver";
    private final String driverVersion = "1.0";
    private final int driverMajorVersion = 1;
    private final int driverMinorVersion = 0;

    public RookieDBDatabaseMetaData(RookieDBConnection connection) {
        this.connection = connection;
    }

    // 数据库产品信息
    @Override
    public String getDatabaseProductName() throws SQLException {
        return databaseProductName;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return databaseProductVersion;
    }

    @Override
    public String getDriverName() throws SQLException {
        return driverName;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return driverVersion;
    }

    @Override
    public int getDriverMajorVersion() {
        return driverMajorVersion;
    }

    @Override
    public int getDriverMinorVersion() {
        return driverMinorVersion;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    // JDBC 版本信息
    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4; // JDBC 4.0
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    // 数据库功能支持
    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false; // RookieDB 不支持并发事务
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false; // RookieDB 不支持外连接
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false; // RookieDB 不支持存储过程
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false; // RookieDB 不支持子查询
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    // SQL 语法支持
    @Override
    public boolean supportsUnion() throws SQLException {
        return false; // RookieDB 不支持 UNION
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    // 结果集类型支持
    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY &&
                concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
    }

    // 事务隔离级别
    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_SERIALIZABLE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    // 标识符相关
    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\""; // 双引号
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "SELECT,FROM,WHERE,INSERT,UPDATE,DELETE,CREATE,DROP,TABLE,INDEX," +
                "PRIMARY,KEY,FOREIGN,REFERENCES,NOT,NULL,UNIQUE,AND,OR,ORDER,BY," +
                "GROUP,HAVING,INNER,JOIN,ON,AS,DISTINCT,COUNT,SUM,AVG,MIN,MAX";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "ABS,CEIL,FLOOR,ROUND,SQRT";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "CONCAT,LENGTH,LOWER,UPPER,SUBSTRING,TRIM";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    // 大小写敏感性
    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    // 最大长度限制
    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0; // 不支持二进制字面量
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 1000;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 128;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 16;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 16;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 16;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 1000;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 1000;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 1; // RookieDB 单连接
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 128;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 1000;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0; // 不支持 schema
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0; // 不支持存储过程
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0; // 不支持 catalog
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 8192;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 65536;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 128;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 16;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 128;
    }

    // 表和列的元数据查询
    @Override
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern, String[] types) throws SQLException {
        // 创建表元数据的结果集
        List<String> columnNames = List.of(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
                "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
        );

        List<String> columnTypes = List.of(
                "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR",
                "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR",
                "VARCHAR", "VARCHAR"
        );

        // 获取数据库中的表信息
        List<List<Object>> rows = new ArrayList<>();

        // 这里需要从 RookieDB 获取实际的表信息
        // 暂时返回空结果集，实际实现需要查询数据库

        return createMetaDataResultSet(columnNames, columnTypes, rows);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern, String columnNamePattern) throws SQLException {
        // 创建列元数据的结果集
        List<String> columnNames = List.of(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
                "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
        );

        List<String> columnTypes = List.of(
                "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR",
                "INTEGER", "VARCHAR", "INTEGER", "INTEGER",
                "INTEGER", "INTEGER", "INTEGER", "VARCHAR",
                "VARCHAR", "INTEGER", "INTEGER", "INTEGER",
                "INTEGER", "VARCHAR", "VARCHAR", "VARCHAR",
                "VARCHAR", "SMALLINT", "VARCHAR", "VARCHAR"
        );

        List<List<Object>> rows = new ArrayList<>();

        // 这里需要从 RookieDB 获取实际的列信息
        // 暂时返回空结果集，实际实现需要查询数据库

        return createMetaDataResultSet(columnNames, columnTypes, rows);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        List<String> columnNames = List.of(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "KEY_SEQ", "PK_NAME"
        );

        List<String> columnTypes = List.of(
                "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR",
                "SMALLINT", "VARCHAR"
        );

        List<List<Object>> rows = new ArrayList<>();

        // 这里需要从 RookieDB 获取实际的主键信息

        return createMetaDataResultSet(columnNames, columnTypes, rows);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique, boolean approximate) throws SQLException {
        List<String> columnNames = List.of(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION",
                "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"
        );

        List<String> columnTypes = List.of(
                "VARCHAR", "VARCHAR", "VARCHAR", "BOOLEAN",
                "VARCHAR", "VARCHAR", "SMALLINT", "SMALLINT",
                "VARCHAR", "VARCHAR", "BIGINT", "BIGINT", "VARCHAR"
        );

        List<List<Object>> rows = new ArrayList<>();

        // 这里需要从 RookieDB 获取实际的索引信息

        return createMetaDataResultSet(columnNames, columnTypes, rows);
    }

    // 辅助方法：创建元数据结果集
    private ResultSet createMetaDataResultSet(List<String> columnNames,
                                              List<String> columnTypes,
                                              List<List<Object>> rows) throws SQLException {
        // 创建 Schema
        List<String> fieldNames = new ArrayList<>(columnNames);
        List<edu.berkeley.cs186.database.databox.Type> fieldTypes = new ArrayList<>();

        for (String type : columnTypes) {
            switch (type) {
                case "VARCHAR":
                    fieldTypes.add(edu.berkeley.cs186.database.databox.Type.stringType(255));
                    break;
                case "INTEGER":
                    fieldTypes.add(edu.berkeley.cs186.database.databox.Type.intType());
                    break;
                case "SMALLINT":
                    fieldTypes.add(edu.berkeley.cs186.database.databox.Type.intType());
                    break;
                case "BIGINT":
                    fieldTypes.add(edu.berkeley.cs186.database.databox.Type.longType());
                    break;
                case "BOOLEAN":
                    fieldTypes.add(edu.berkeley.cs186.database.databox.Type.boolType());
                    break;
                default:
                    fieldTypes.add(edu.berkeley.cs186.database.databox.Type.stringType(255));
            }
        }

        edu.berkeley.cs186.database.table.Schema schema =
                new edu.berkeley.cs186.database.table.Schema();
        for (int i = 0; i < fieldNames.size(); i++) {
            schema.add(fieldNames.get(i), fieldTypes.get(i));
        }


        // 转换数据行
        List<edu.berkeley.cs186.database.table.Record> records = new ArrayList<>();
        for (List<Object> row : rows) {
            List<edu.berkeley.cs186.database.databox.DataBox> values = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                Object value = row.get(i);
                if (value == null) {
                    values.add(null);
                } else {
                    values.add(edu.berkeley.cs186.database.databox.DataBox.fromObject(value));
                }
            }
            records.add(new edu.berkeley.cs186.database.table.Record(values));
        }

        return new RookieDBResultSet(records, schema, null);
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    // 其他必需的方法实现（简化版本）
    @Override
    public String getURL() throws SQLException {
        return connection.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    // 包装器方法
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

    // 其他未实现的方法可以抛出 SQLFeatureNotSupportedException
    @Override
    public ResultSet getSchemas() throws SQLException {
        throw new SQLFeatureNotSupportedException("Schemas not supported");
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        throw new SQLFeatureNotSupportedException("Catalogs not supported");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<String> columnNames = List.of("TABLE_TYPE");
        List<String> columnTypes = List.of("VARCHAR");
        List<List<Object>> rows = List.of(
                List.of("TABLE")
        );
        return createMetaDataResultSet(columnNames, columnTypes, rows);
    }

}
