package edu.berkeley.cs186.database.jdbc;

import edu.berkeley.cs186.database.jdbc.driver.RookieDBDriver;
import edu.berkeley.cs186.database.jdbc.pool.RookieDBDataSource;
import edu.berkeley.cs186.database.jdbc.pool.RookieDBPoolConfiguration;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RookieDBDataSourceIntegrationTest {

    private static final String TEST_DB_PATH = "./test_pool_db";
    private static final Logger log = LoggerFactory.getLogger(RookieDBDataSourceIntegrationTest.class);

    private static RookieDBDataSource dataSource;
    private static String jdbcUrl;



    @BeforeAll
    static void setUpClass() throws Exception {
        log.info("初始化连接池集成测试环境...");
        
        // 注册JDBC驱动
        Class.forName(RookieDBDriver.class.getName());
        
        // 清理并创建测试数据库目录
        cleanupDatabase();
        Files.createDirectories(Paths.get(TEST_DB_PATH));
        
        log.info("连接池集成测试环境初始化完成");

        jdbcUrl = "jdbc:rookiedb:" + TEST_DB_PATH;

        // 为每个测试创建新的DataSource
        RookieDBPoolConfiguration config = new RookieDBPoolConfiguration();
        dataSource = new RookieDBDataSource(jdbcUrl, config);
    }

    @AfterAll
    static void tearDownClass() throws Exception {
        log.info("清理连接池集成测试环境...");
        if (dataSource != null) {
            dataSource.close();
        }
        // 等待所有连接关闭
        Thread.sleep(200);
        
        // 清理测试数据库
        cleanupDatabase();
        
        log.info("连接池集成测试环境清理完成");
    }

    @BeforeEach
    void setUp() throws Exception {

    }

    @AfterEach
    void tearDown() throws Exception {

    }

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



    // ==================== 基础连接池测试 ====================

    @Test
    @Order(1)
    void testDataSourceCreation() throws Exception {
        log.info("测试DataSource创建...");
        
        assertNotNull(dataSource, "DataSource不应为null");
        
        // 测试获取连接
        try (Connection conn = dataSource.getConnection()) {
            assertNotNull(conn, "连接不应为null");
            assertFalse(conn.isClosed(), "连接应该是打开的");
            
            // 测试连接功能
            try (Statement stmt = conn.createStatement()) {

                stmt.execute("CREATE TABLE test_table (id INT, name STRING50)");
                stmt.executeUpdate("INSERT INTO test_table VALUES (1, 'test')");
                
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM test_table")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("count"));
                }
            }
        }
        
        log.info("DataSource创建测试通过");
    }

    @Test
    @Order(2)
    void testDefaultConfiguration() throws Exception {
        log.info("测试默认配置...");
        
        // 使用默认配置创建DataSource
        try (RookieDBDataSource defaultDataSource = new RookieDBDataSource(jdbcUrl)) {
            assertNotNull(defaultDataSource);
            
            try (Connection conn = defaultDataSource.getConnection()) {
                assertNotNull(conn);
                assertFalse(conn.isClosed());
            }
        }
        
        log.info("默认配置测试通过");
    }

    @Test
    @Order(3)
    void testCustomConfiguration() throws Exception {
        log.info("测试自定义配置...");
        
        // 创建自定义配置
        RookieDBPoolConfiguration customConfig = new RookieDBPoolConfiguration();
        // 注意：由于RookieDBPoolConfiguration没有公开setter方法，
        // 这里只能测试默认配置的有效性
        
        try (RookieDBDataSource customDataSource = new RookieDBDataSource(jdbcUrl, customConfig)) {
            assertNotNull(customDataSource);
            
            try (Connection conn = customDataSource.getConnection()) {
                assertNotNull(conn);
                assertFalse(conn.isClosed());
            }
        }
        
        log.info("自定义配置测试通过");
    }

    // ==================== 连接池功能测试 ====================

    @Test
    @Order(4)
    void testMultipleConnections() throws Exception {
        log.info("测试多连接获取...");
        
        List<Connection> connections = new ArrayList<>();
        int i = 0;
        try {
            // 获取多个连接
            for (; i < 5; i++) {
                Connection conn = dataSource.getConnection();
                assertNotNull(conn);
                assertFalse(conn.isClosed());
                connections.add(conn);
                
                // 测试每个连接都能正常工作
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE  multi_test_" + i + " (id INT)");
                    stmt.executeUpdate("INSERT INTO multi_test_" + i + " VALUES (" + i + ")");
                }
            }
            
            assertEquals(5, connections.size(), "应该成功获取5个连接");
            
        } finally {
            // 关闭所有连接
            for (Connection conn : connections) {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            }
        }
        connections.clear();
        try {
            // 获取多个连接
            for (; i < 10; i++) {
                Connection conn = dataSource.getConnection();
                assertNotNull(conn);
                assertFalse(conn.isClosed());
                connections.add(conn);

                // 测试每个连接都能正常工作
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE  multi_test_" + i + " (id INT)");
                    stmt.executeUpdate("INSERT INTO multi_test_" + i + " VALUES (" + i + ")");
                }
            }

            assertEquals(5, connections.size(), "应该成功获取5个连接");

        } finally {
            // 关闭所有连接
            for (Connection conn : connections) {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            }
        }
        
        log.info("多连接获取测试通过");
    }

    @Test
    @Order(5)
    void testConnectionReuse() throws Exception {
        log.info("测试连接复用...");
        
        // 第一次获取连接并使用
        try (Connection conn1 = dataSource.getConnection()) {
            try (Statement stmt = conn1.createStatement()) {
                stmt.execute("CREATE TABLE reuse_test (id INT, value STRING50)");
                stmt.executeUpdate("INSERT INTO reuse_test VALUES (1, 'first')");
            }
        }
        
        // 第二次获取连接，应该能看到之前的数据（连接池复用）
        try (Connection conn2 = dataSource.getConnection()) {
            try (Statement stmt = conn2.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM reuse_test")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("count"));
            }
        }
        
        log.info("连接复用测试通过");
    }

    // ==================== 并发测试 ====================

    @Test
    @Order(6)
    void testConcurrentAccess() throws Exception {
        log.info("测试并发访问...");

        
        int threadCount = 10;
        int operationsPerThread = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        // 创建测试表
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE concurrent_test (id INT, thread_id INT, operation_id INT)");
        }
        
        // 提交并发任务
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        try (Connection conn = dataSource.getConnection();
                             Statement stmt = conn.createStatement()) {
                            Thread.sleep(10);
                            stmt.executeUpdate("INSERT INTO concurrent_test VALUES (" +
                                (threadId * operationsPerThread + j) + ", " + threadId + ", " + j + ")");
                            successCount.incrementAndGet();
                            
                            // 模拟一些处理时间
                            Thread.sleep(10);
                        }
                    }
                } catch (Exception e) {
                    log.error("线程 {} 执行失败: {}", threadId, e.getMessage());
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有任务完成
        assertTrue(latch.await(300, TimeUnit.SECONDS), "并发测试应该在30秒内完成");
        executor.shutdown();
        
        // 验证结果
        assertEquals(0, errorCount.get(), "不应该有错误发生");
        assertEquals(threadCount * operationsPerThread, successCount.get(), "所有操作都应该成功");
        
        // 验证数据完整性
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM concurrent_test")) {
            assertTrue(rs.next());
            assertEquals(threadCount * operationsPerThread, rs.getInt("count"));
        }
        
        log.info("并发访问测试通过");
    }

    @Test
    @Order(9)
    void testLargeInsert() throws SQLException {
        log.info("开始大数据量插入测试...");

        final int RECORD_COUNT = 20000;
        final String TABLE_NAME = "monitoring_data";

        try (Connection conn = dataSource.getConnection()) {
            // 1. 创建监控表
            log.info("创建监控表: {}", TABLE_NAME);
            try (Statement stmt = conn.createStatement()) {
                // 删除表如果存在
                try {
                    stmt.execute("DROP TABLE " + TABLE_NAME);
                } catch (SQLException e) {
                    // 忽略表不存在的错误
                }

                // 创建监控数据表
                String createTableSQL =
                        "CREATE TABLE " + TABLE_NAME + " (" +
                                "id INT, " +
                                "timestamp LONG, " +
                                "metric_name STRING50, " +
                                "metric_value FLOAT, " +
                                "host_name STRING30, " +
                                "status STRING10" +
                                ")";
                stmt.execute(createTableSQL);
                log.info("监控表创建成功");
            }

            // 2. 准备时序数据 - 12小时时间范围
            long baseTimestamp = System.currentTimeMillis();
            long twelveHoursInMillis = 12 * 60 * 60 * 1000L; // 12小时的毫秒数

            String[] metricNames = {"cpu_usage", "memory_usage", "disk_io", "network_io", "response_time"};
            String[] hostNames = {"web-01", "web-02", "db-01", "db-02", "cache-01"};
            String[] statuses = {"normal", "warning", "critical"};

            // 3. 批量插入数据并测量时间
            log.info("开始插入{}条时序数据...", RECORD_COUNT);
            long insertStartTime = System.currentTimeMillis();

            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO " + TABLE_NAME  +
                            " VALUES (?, ?, ?, ?, ?, ?)")) {

                // 使用批处理提高插入性能
                conn.setAutoCommit(false);

                for (int i = 0; i < RECORD_COUNT; i++) {
                    // 生成随机时间戳（12小时范围内）
                    long randomTimestamp = baseTimestamp + (long)(Math.random() * twelveHoursInMillis);

                    // 生成随机监控数据
                    String metricName = metricNames[(int)(Math.random() * metricNames.length)];
                    float metricValue = (float)(Math.random() * 100); // 0-100的随机值
                    String hostName = hostNames[(int)(Math.random() * hostNames.length)];
                    String status = statuses[(int)(Math.random() * statuses.length)];

                    pstmt.setInt(1, i + 1);
                    pstmt.setLong(2, randomTimestamp);
                    pstmt.setString(3, metricName);
                    pstmt.setFloat(4, metricValue);
                    pstmt.setString(5, hostName);
                    pstmt.setString(6, status);

                    pstmt.addBatch();

                    // 每1000条提交一次
                    if ((i + 1) % 1000 == 0) {
                        pstmt.executeBatch();
                        conn.commit();
                        log.debug("已插入{}条记录", i + 1);
                    }
                }

                // 提交剩余的记录
                pstmt.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);
            }

            long insertEndTime = System.currentTimeMillis();
            long insertDuration = insertEndTime - insertStartTime;

            log.info("插入{}条记录完成，耗时: {}ms ({}秒)",
                    RECORD_COUNT, insertDuration, insertDuration / 1000.0);

            // 4. 验证插入的数据量
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM " + TABLE_NAME)) {
                    assertTrue(rs.next());
                    int actualCount = rs.getInt("total");
                    assertEquals(RECORD_COUNT, actualCount, "插入的记录数量不匹配");
                    log.info("数据验证通过，实际插入记录数: {}", actualCount);
                }
            }

            // 5. 测试时间范围查询性能
            log.info("开始测试时间范围查询...");

            // 查询最近6小时的数据
            long sixHoursAgo = baseTimestamp + (6 * 60 * 60 * 1000L);

            long queryStartTime = System.currentTimeMillis();

            try (PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT id, timestamp, metric_name, metric_value, host_name, status " +
                            "FROM " + TABLE_NAME + " WHERE timestamp > ?")) {

                pstmt.setLong(1, sixHoursAgo);

                try (ResultSet rs = pstmt.executeQuery()) {
                    int queryResultCount = 0;
                    while (rs.next()) {
                        queryResultCount++;

                        // 验证查询结果的时间戳确实大于指定时间
                        long resultTimestamp = rs.getLong("timestamp");
                        assertTrue(resultTimestamp > sixHoursAgo,
                                "查询结果中存在时间戳不符合条件的记录");
                    }

                    long queryEndTime = System.currentTimeMillis();
                    long queryDuration = queryEndTime - queryStartTime;

                    log.info("时间范围查询完成:");
                    log.info("  - 查询条件: timestamp > {} (最近6小时)", sixHoursAgo);
                    log.info("  - 查询结果: {}条记录", queryResultCount);
                    log.info("  - 查询耗时: {}ms", queryDuration);

                    // 验证查询结果数量合理（应该大于0，小于总数）
                    assertTrue(queryResultCount > 0, "查询结果不应为空");
                    assertTrue(queryResultCount < RECORD_COUNT, "查询结果应该少于总记录数");
                }
            }

            // 6. 测试聚合查询性能
            log.info("开始测试聚合查询...");

            long aggQueryStartTime = System.currentTimeMillis();

            try (Statement stmt = conn.createStatement()) {
                // 按主机名统计记录数
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT host_name, COUNT(*) as record_count " +
                                "FROM " + TABLE_NAME + " " +
                                "GROUP BY host_name")) {

                    int hostCount = 0;
                    while (rs.next()) {
                        String hostName = rs.getString("host_name");
                        int recordCount = rs.getInt("record_count");
                        log.debug("主机 {} 有 {} 条记录", hostName, recordCount);
                        hostCount++;
                    }

                    long aggQueryEndTime = System.currentTimeMillis();
                    long aggQueryDuration = aggQueryEndTime - aggQueryStartTime;

                    log.info("聚合查询完成:");
                    log.info("  - 查询类型: 按主机名分组统计");
                    log.info("  - 主机数量: {}", hostCount);
                    log.info("  - 查询耗时: {}ms", aggQueryDuration);

                    assertEquals(hostNames.length, hostCount, "主机数量应该匹配");
                }
            }

            // 7. 性能基准测试
            double insertRate = (double) RECORD_COUNT / (insertDuration / 1000.0);
            log.info("性能统计:");
            log.info("  - 插入速率: {} 记录/秒", insertRate);
            log.info("  - 平均插入时间: {} ms/记录", (double) insertDuration / RECORD_COUNT);

            // 性能断言（根据实际环境调整）
            assertTrue(insertRate > 100, "插入速率应该大于100记录/秒");
            assertTrue(insertDuration < 60000, "总插入时间应该少于60秒");

        } catch (SQLException e) {
            log.error("大数据量插入测试失败: {}", e.getMessage(), e);
            throw e;
        }

        log.info("大数据量插入测试完成");
    }

    // ==================== 事务测试 ====================

    @Test
    @Order(9)
    void testTransactionWithPool() throws Exception {
        log.info("测试连接池中的事务...");
        
        // 创建测试表
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE transaction_pool_test (id INT, value STRING50)");
        }
        
        // 测试事务提交
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("INSERT INTO transaction_pool_test VALUES (1, 'committed')");
                stmt.executeUpdate("INSERT INTO transaction_pool_test VALUES (2, 'committed')");
                conn.commit();
            }
        }
        
        // 验证提交的数据
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM transaction_pool_test")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("count"));
        }
        
        // 测试事务回滚
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("INSERT INTO transaction_pool_test VALUES (3, 'rollback')");
                stmt.executeUpdate("INSERT INTO transaction_pool_test VALUES (4, 'rollback')");
                conn.rollback();
            }
        }
        
        // 验证回滚后的数据
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM transaction_pool_test")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("count"), "回滚后应该还是2条记录");
        }
        
        log.info("连接池事务测试通过");
    }



    // ==================== 综合集成测试 ====================

    @Test
    @Order(11)
    void testFullPoolIntegration() throws Exception {
        log.info("执行完整连接池集成测试...");
        
        // 创建测试表
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE pool_integration_test (id INT, name STRING50, created_by STRING20)");
        }
        
        // 模拟多个客户端同时使用连接池
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(5);
        AtomicInteger totalInserts = new AtomicInteger(0);
        
        for (int i = 0; i < 5; i++) {
            final int clientId = i;
            executor.submit(() -> {
                try {
                    // 每个客户端执行多个操作
                    for (int j = 0; j < 3; j++) {
                        try (Connection conn = dataSource.getConnection()) {
                            conn.setAutoCommit(false);
                            
                            try (Statement stmt = conn.createStatement()) {
                                int id = clientId * 10 + j;
                                stmt.executeUpdate("INSERT INTO pool_integration_test VALUES (" + 
                                    id + ", 'Client" + clientId + "_Record" + j + "', 'Client" + clientId + "')");
                                
                                // 模拟一些处理时间
                                Thread.sleep(50);
                                
                                conn.commit();
                                totalInserts.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("客户端 {} 执行失败: {}", clientId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有客户端完成
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        
        // 验证最终结果
        assertEquals(15, totalInserts.get(), "应该插入15条记录");
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM pool_integration_test")) {
            assertTrue(rs.next());
            assertEquals(15, rs.getInt("count"));
        }
        
        // 验证数据完整性
//        try (Connection conn = dataSource.getConnection();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery("SELECT DISTINCT created_by FROM pool_integration_test ORDER BY created_by")) {
//
//            int clientCount = 0;
//            while (rs.next()) {
//                String createdBy = rs.getString("created_by");
//                assertTrue(createdBy.startsWith("Client"));
//                clientCount++;
//            }
//            assertEquals(5, clientCount, "应该有5个不同的客户端");
//        }
//
//        log.info("完整连接池集成测试通过");
    }

    // ==================== 错误处理测试 ====================

    @Test
    @Order(12)
    void testDataSourceClosure() throws Exception {
        log.info("测试DataSource关闭...");

        // 获取连接并验证正常工作
        try (Connection conn = dataSource.getConnection()) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }

        // 关闭DataSource
        dataSource.close();

        // 尝试获取连接应该失败
        assertThrows(SQLException.class, () -> {
            dataSource.getConnection();
        }, "关闭的DataSource不应该能获取连接");

        // 重新创建DataSource用于后续测试
        RookieDBPoolConfiguration config = new RookieDBPoolConfiguration();
        dataSource = new RookieDBDataSource(jdbcUrl, config);

        log.info("DataSource关闭测试通过");
    }
}