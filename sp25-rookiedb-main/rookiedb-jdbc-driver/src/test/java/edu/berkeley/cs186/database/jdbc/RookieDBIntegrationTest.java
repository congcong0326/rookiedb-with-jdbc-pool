package edu.berkeley.cs186.database.jdbc;

import edu.berkeley.cs186.database.jdbc.driver.RookieDBConnection;
import edu.berkeley.cs186.database.jdbc.driver.RookieDBDriver;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RookieDBIntegrationTest {


    private static final String TEST_DB_PATH = "./test_integration_db";
    private static final Logger log = LoggerFactory.getLogger(RookieDBIntegrationTest.class);

    // 共享数据库连接
    private static Connection connection;
    private Statement statement;

    // 跟踪每个测试创建的表，用于清理
    private Set<String> createdTables = new HashSet<>();

    @BeforeAll
    static void setUpClass() throws Exception {
        log.info("初始化集成测试环境...");

        // 注册JDBC驱动
        Class.forName(RookieDBDriver.class.getName());

        // 清理并创建测试数据库目录
        cleanupDatabase();
        Files.createDirectories(Paths.get(TEST_DB_PATH));

        // 创建共享数据库连接
        String url = "jdbc:rookiedb:" + TEST_DB_PATH;
        Properties props = new Properties();
        props.setProperty("bufferSize", "64");
        props.setProperty("enableLocking", "true");
        props.setProperty("enableRecovery", "true");

        connection = DriverManager.getConnection(url, props);
        log.info("共享数据库连接建立成功: {}", url);
    }

    @AfterAll
    static void tearDownClass() throws Exception {
        log.info("清理集成测试环境...");

        if (connection != null && !connection.isClosed()) {
            connection.close();
        }

        // 等待连接完全关闭
        Thread.sleep(100);

        // 清理测试数据库
        cleanupDatabase();

        log.info("集成测试环境清理完成");
    }

    @BeforeEach
    void setUp() throws Exception {
        log.info("设置测试用例: {}", getCurrentTestName());

        // 为每个测试创建新的Statement
        statement = connection.createStatement();

        // 确保连接处于自动提交模式
        if (!connection.getAutoCommit()) {
            connection.rollback();
            connection.setAutoCommit(true);
        }

        // 清空创建的表记录
        createdTables.clear();
    }

    @AfterEach
    void tearDown() throws Exception {
        log.info("清理测试用例: {}", getCurrentTestName());

        try {
            // 清理本测试创建的所有表
            for (String tableName : createdTables) {
                try {
                    statement.execute("DROP TABLE " + tableName);
                    log.debug("删除表: {}", tableName);
                } catch (SQLException e) {
                    log.warn("删除表 {} 失败: {}", tableName, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("清理表时出现异常: {}", e.getMessage());
        } finally {
            // 关闭Statement
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        }
    }

    // 辅助方法：记录创建的表
    private void createTable(String sql) throws SQLException {
        statement.execute(sql);
        // 从SQL中提取表名
        String tableName = extractTableName(sql);
        if (tableName != null) {
            createdTables.add(tableName);
            log.debug("记录创建的表: {}", tableName);
        }
    }

    // 从CREATE TABLE语句中提取表名
    private String extractTableName(String sql) {
        try {
            String upperSql = sql.toUpperCase().trim();
            if (upperSql.startsWith("CREATE TABLE")) {
                String[] parts = upperSql.split("\\s+");
                if (parts.length >= 3) {
                    return parts[2].split("\\(")[0].trim();
                }
            }
        } catch (Exception e) {
            log.warn("提取表名失败: {}", e.getMessage());
        }
        return null;
    }

    // 获取当前测试方法名
    private String getCurrentTestName() {
        return Thread.currentThread().getStackTrace()[3].getMethodName();
    }

    private static void cleanupDatabase() throws Exception {
        Path dbPath = Paths.get(TEST_DB_PATH);
        if (Files.exists(dbPath)) {
            Files.walk(dbPath)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    // ==================== 基础连接测试 ====================

    @Test
    @Order(1)
    void testDatabaseConnection() throws SQLException {
        log.info("测试数据库连接...");

        assertNotNull(connection, "数据库连接不应为null");
        assertFalse(connection.isClosed(), "数据库连接应该是打开的");
        assertTrue(connection instanceof RookieDBConnection, "连接应该是RookieDBConnection实例");

        // 测试连接元数据
        DatabaseMetaData metaData = connection.getMetaData();
        assertNotNull(metaData, "数据库元数据不应为null");
        assertEquals("RookieDB", metaData.getDatabaseProductName());

        log.info("数据库连接测试通过");
    }

    // ==================== DDL 测试 ====================

    @Test
    @Order(2)
    void testCreateAndDropTable() throws SQLException {
        log.info("测试创建和删除表...");

        // 创建表
        String createSQL = "CREATE TABLE users (id INT, name STRING50, email STRING100, age INT)";
        createTable(createSQL);

        // 验证表是否创建成功 - 通过插入数据来验证
        statement.executeUpdate("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com', 25)");

        ResultSet rs = statement.executeQuery("SELECT COUNT(*) as count FROM users");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("count"));
        rs.close();

        log.info("创建和删除表测试通过");
    }

    @Test
    @Order(3)
    void testCreateIndex() throws SQLException {
        log.info("测试创建索引...");

        // 先创建表
        createTable("CREATE TABLE indexed_table (id INT, name STRING50, score INT)");

        // 插入一些测试数据
        statement.executeUpdate("INSERT INTO indexed_table VALUES (1, 'Alice', 85)");
        statement.executeUpdate("INSERT INTO indexed_table VALUES (2, 'Bob', 92)");
        statement.executeUpdate("INSERT INTO indexed_table VALUES (3, 'Charlie', 78)");

        // 创建索引
        boolean indexResult = statement.execute("CREATE INDEX ON indexed_table(score)");
        assertFalse(indexResult, "CREATE INDEX应该返回false");

        // 验证索引创建后查询仍然正常工作
        ResultSet rs = statement.executeQuery("SELECT name FROM indexed_table WHERE score > 80 ORDER BY score ASC");

        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("name"));
        assertFalse(rs.next());
        rs.close();

        log.info("创建索引测试通过");
    }

    // ==================== DML 测试 ====================

    @Test
    @Order(4)
    void testInsertUpdateDelete() throws SQLException {
        log.info("测试插入、更新、删除操作...");

        // 创建测试表
        createTable("CREATE TABLE products (id INT, name STRING100, price INT, category STRING50)");

        // 测试插入
        int insertCount1 = statement.executeUpdate("INSERT INTO products VALUES (1, 'Laptop', 1000, 'Electronics')");
        assertEquals(1, insertCount1, "插入应该影响1行");

        int insertCount2 = statement.executeUpdate("INSERT INTO products VALUES (2, 'Mouse', 25, 'Electronics')");
        assertEquals(1, insertCount2, "插入应该影响1行");

        int insertCount3 = statement.executeUpdate("INSERT INTO products VALUES (3, 'Book', 15, 'Education')");
        assertEquals(1, insertCount3, "插入应该影响1行");

        // 验证插入结果
        ResultSet rs = statement.executeQuery("SELECT COUNT(*) as count FROM products");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("count"));
        rs.close();

        // 测试更新
        int updateCount = statement.executeUpdate("UPDATE products SET price = 1200 WHERE id = 1");
        assertEquals(1, updateCount, "更新应该影响1行");

        // 验证更新结果
        rs = statement.executeQuery("SELECT price FROM products WHERE id = 1");
        assertTrue(rs.next());
        assertEquals(1200, rs.getInt("price"));
        rs.close();

        // 测试删除
        int deleteCount = statement.executeUpdate("DELETE FROM products WHERE category = 'Education'");
        assertEquals(1, deleteCount, "删除应该影响1行");

        // 验证删除结果
        rs = statement.executeQuery("SELECT COUNT(*) as count FROM products");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("count"));
        rs.close();

        log.info("插入、更新、删除操作测试通过");
    }

    // ==================== 查询测试 ====================

    @Test
    @Order(5)
    void testComplexQueries() throws SQLException {
        log.info("测试复杂查询...");

        // 创建测试表
        statement.execute("CREATE TABLE employees (id INT, name STRING50, department STRING50, salary INT, hire_date STRING20)");
        statement.execute("CREATE TABLE departments (id INT, name STRING50, budget INT)");

        // 插入测试数据
        statement.executeUpdate("INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 75000, '2023-01-15')");
        statement.executeUpdate("INSERT INTO employees VALUES (2, 'Bob Smith', 'Marketing', 65000, '2023-02-20')");
        statement.executeUpdate("INSERT INTO employees VALUES (3, 'Charlie Brown', 'Engineering', 80000, '2023-03-10')");
        statement.executeUpdate("INSERT INTO employees VALUES (4, 'Diana Prince', 'HR', 70000, '2023-04-05')");
        statement.executeUpdate("INSERT INTO employees VALUES (5, 'Eve Wilson', 'Engineering', 85000, '2023-05-12')");

        statement.executeUpdate("INSERT INTO departments VALUES (1, 'Engineering', 500000)");
        statement.executeUpdate("INSERT INTO departments VALUES (2, 'Marketing', 300000)");
        statement.executeUpdate("INSERT INTO departments VALUES (3, 'HR', 200000)");

        // 测试WHERE条件查询
        ResultSet rs = statement.executeQuery("SELECT name, salary FROM employees WHERE salary >= 70000 ORDER BY salary DESC");

        int count = 0;
        while (rs.next()) {
            count++;
            assertTrue(rs.getInt("salary") >= 70000, "查询结果应该满足WHERE条件");
        }
        assertEquals(4, count, "应该返回4条记录");
        rs.close();

        // 测试聚合函数
        rs = statement.executeQuery("SELECT COUNT(*) as emp_count FROM employees WHERE department = 'Engineering'");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("emp_count"));
        rs.close();

        log.info("复杂查询测试通过");
    }

    // ==================== 事务测试 ====================

    @Test
    @Order(6)
    void testTransactionCommit() throws SQLException {
        log.info("测试事务提交...");

        connection.setAutoCommit(false);

        try {
            // 创建表
            statement.execute("CREATE TABLE transaction_test (id INT, value STRING50)");

            // 插入数据
            statement.executeUpdate("INSERT INTO transaction_test VALUES (1, 'test1')");
            statement.executeUpdate("INSERT INTO transaction_test VALUES (2, 'test2')");
            statement.executeUpdate("INSERT INTO transaction_test VALUES (3, 'test3')");

            // 提交事务
            connection.commit();

            // 验证数据已提交
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) as count FROM transaction_test");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("count"));
            rs.close();

        } finally {
            connection.setAutoCommit(true);
        }

        log.info("事务提交测试通过");
    }

    @Test
    @Order(7)
    void testTransactionRollback() throws SQLException {
        log.info("测试事务回滚...");

        // 先创建表并提交
        statement.execute("CREATE TABLE rollback_test (id INT, value STRING50)");

        connection.setAutoCommit(false);

        try {
            // 插入一些初始数据并提交
            statement.executeUpdate("INSERT INTO rollback_test VALUES (1, 'committed')");
            connection.commit();

            // 插入更多数据但不提交
            statement.executeUpdate("INSERT INTO rollback_test VALUES (2, 'rollback1')");
            statement.executeUpdate("INSERT INTO rollback_test VALUES (3, 'rollback2')");

            // 回滚事务
            connection.rollback();

            // 验证只有提交的数据存在
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) as count FROM rollback_test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("count"), "回滚后应该只有1条已提交的记录");
            rs.close();

            rs = statement.executeQuery("SELECT value FROM rollback_test");
            assertTrue(rs.next());
            assertEquals("committed", rs.getString("value"));
            assertFalse(rs.next());
            rs.close();

        } finally {
            connection.setAutoCommit(true);
        }

        log.info("事务回滚测试通过");
    }

    // ==================== 错误处理测试 ====================

    @Test
    @Order(8)
    void testErrorHandling() throws SQLException {
        log.info("测试错误处理...");

        // 测试语法错误
        assertThrows(SQLException.class, () -> {
            statement.executeQuery("SELCT * FROM nonexistent"); // 故意拼错SELECT
        }, "语法错误应该抛出SQLException");

        // 测试查询不存在的表
        assertThrows(SQLException.class, () -> {
            statement.executeQuery("SELECT * FROM nonexistent_table");
        }, "查询不存在的表应该抛出SQLException");

        // 创建表用于测试列错误
        statement.execute("CREATE TABLE error_test (id INT, name STRING50)");

        // 测试查询不存在的列
        assertThrows(SQLException.class, () -> {
            statement.executeQuery("SELECT nonexistent_column FROM error_test");
        }, "查询不存在的列应该抛出SQLException");

        log.info("错误处理测试通过");
    }

    // ==================== 性能测试 ====================

    @Test
    @Order(9)
    void testPerformance() throws SQLException {
        log.info("测试性能...");

        // 创建表
        statement.execute("CREATE TABLE performance_test (id INT, data STRING100, timestamp STRING20)");

        // 批量插入测试
        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= 500; i++) {
            statement.executeUpdate("INSERT INTO performance_test VALUES (" + i + ", 'data" + i + "', '2023-01-01')");
        }

        long insertTime = System.currentTimeMillis() - startTime;
        log.info("插入500条记录耗时: {}ms", insertTime);

        // 查询性能测试
        startTime = System.currentTimeMillis();

        ResultSet rs = statement.executeQuery("SELECT COUNT(*) as count FROM performance_test WHERE id > 250");
        assertTrue(rs.next());
        assertEquals(250, rs.getInt("count"));
        rs.close();

        long queryTime = System.currentTimeMillis() - startTime;
        log.info("条件查询耗时: {}ms", queryTime);

        // 性能断言（根据实际情况调整）
        //assertTrue(insertTime < 10000, "插入500条记录应该在10秒内完成");
        assertTrue(queryTime < 1000, "查询应该在1秒内完成");

        log.info("性能测试通过");
    }

    // ==================== 综合集成测试 ====================

    @Test
    @Order(10)
    void testFullIntegrationScenario() throws SQLException {
        log.info("执行完整集成测试场景...");

        // 模拟一个完整的业务场景：学生管理系统

        // 1. 创建相关表
        statement.execute("CREATE TABLE students (id INT, name STRING50, email STRING100, major STRING50)");
        statement.execute("CREATE TABLE courses (id INT, name STRING100, credits INT, instructor STRING50)");
        statement.execute("CREATE TABLE enrollments (student_id INT, course_id INT, grade STRING2, semester STRING20)");

        // 2. 插入基础数据
        statement.executeUpdate("INSERT INTO students VALUES (1, 'Alice Johnson', 'alice@university.edu', 'Computer Science')");
        statement.executeUpdate("INSERT INTO students VALUES (2, 'Bob Smith', 'bob@university.edu', 'Mathematics')");
        statement.executeUpdate("INSERT INTO students VALUES (3, 'Charlie Brown', 'charlie@university.edu', 'Computer Science')");

        statement.executeUpdate("INSERT INTO courses VALUES (1, 'Database Systems', 3, 'Dr. Wilson')");
        statement.executeUpdate("INSERT INTO courses VALUES (2, 'Algorithms', 4, 'Dr. Johnson')");
        statement.executeUpdate("INSERT INTO courses VALUES (3, 'Linear Algebra', 3, 'Dr. Smith')");

        // 3. 创建选课记录
        statement.executeUpdate("INSERT INTO enrollments VALUES (1, 1, 'A', 'Fall 2023')");
        statement.executeUpdate("INSERT INTO enrollments VALUES (1, 2, 'B', 'Fall 2023')");
        statement.executeUpdate("INSERT INTO enrollments VALUES (2, 2, 'A', 'Fall 2023')");
        statement.executeUpdate("INSERT INTO enrollments VALUES (2, 3, 'A', 'Fall 2023')");
        statement.executeUpdate("INSERT INTO enrollments VALUES (3, 1, 'B', 'Fall 2023')");

        // 4. 执行复杂查询验证数据完整性

        // 查询每个学生的选课情况 - 使用JOIN语法
        ResultSet rs = statement.executeQuery(
                "SELECT s.name as student_name, c.name as course_name, e.grade as grade " +
                        "FROM students AS s " +
                        "JOIN enrollments AS e ON s.id = e.student_id " +
                        "JOIN courses AS c ON c.id = e.course_id " +
                        "ORDER BY s.name, c.name"
        );

        int enrollmentCount = 0;
        while (rs.next()) {
            enrollmentCount++;
            assertNotNull(rs.getString("student_name"));
            assertNotNull(rs.getString("course_name"));
            assertNotNull(rs.getString("grade"));
        }
        assertEquals(5, enrollmentCount, "应该有5条选课记录");
        rs.close();

        // 查询计算机科学专业学生的选课数量 - 使用JOIN语法
        rs = statement.executeQuery(
                "SELECT COUNT(*) as cs_enrollments " +
                        "FROM students s " +
                        "JOIN enrollments e ON s.id = e.student_id " +
                        "WHERE s.major = 'Computer Science'"
        );
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cs_enrollments"));
        rs.close();

        // 5. 测试数据更新
        int updateCount = statement.executeUpdate("UPDATE enrollments SET grade = 'A' WHERE student_id = 3 AND course_id = 1");
        assertEquals(1, updateCount);

        // 6. 验证更新结果
        rs = statement.executeQuery("SELECT grade FROM enrollments WHERE student_id = 3 AND course_id = 1");
        assertTrue(rs.next());
        assertEquals("A", rs.getString("grade"));
        rs.close();

        log.info("完整集成测试场景通过");
    }
}