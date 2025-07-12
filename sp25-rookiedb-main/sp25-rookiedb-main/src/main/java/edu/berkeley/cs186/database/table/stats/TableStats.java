package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.PageDirectory;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Every table in a database maintains a set of table statistics which are
 * updated whenever a tuple is added or deleted to it. These table statistics
 * consist of an estimated number of records in the table, an estimated number
 * of pages used by the table, and a histogram on every column of the table.
 * For example, we can construct a TableStats and add/remove records from
 * it like this:
 *
 *   // Create a TableStats object for a table with columns (x: int, y: float).
 *   List<String> fieldNames = Arrays.asList("x", "y");
 *   List<Type> fieldTypes = Arrays.asList(Type.intType(), Type.floatType());
 *   Schema schema = new Schema(fieldNames, fieldTypes);
 *   TableStats stats = new TableStats(schema);
 *
 *   // Add and remove tuples from the stats.
 *   IntDataBox x1 = new IntDataBox(1);
 *   FloatDataBox y1 = new FloatDataBox(1);
 *   Record r1 = new Record(schema, Arrays.asList(x1, y1));
 *
 *   IntDataBox x2 = new IntDataBox(1);
 *   FloatDataBox y2 = new FloatDataBox(1);
 *   Record r2 = new Record(schema, Arrays.asList(x2, y2));
 *
 *   stats.addRecord(r1);
 *   stats.addRecord(r2);
 *   stats.removeRecord(r1);
 *
 * Later, we can use the statistics maintained by a TableStats object for
 * things like query optimization:
 *
 *   stats.getNumRecords(); // Estimated number of records.
 *   stats.getNumPages();   // Estimated number of pages.
 *   stats.getHistograms(); // Histograms on each column.
 *
 * 统计信息维护
 * - 记录数量统计 ：跟踪表中的记录总数 ( numRecords )
 * - 页面数量估算 ：根据每页记录数计算所需的数据页数量
 * - 列直方图 ：为表的每一列维护一个直方图，用于数据分布分析
 */
public class TableStats {
    private Schema schema;// 表的模式定义
    private int numRecordsPerPage;// 每页记录数
    private int numRecords;// 总记录数
    private List<Histogram> histograms;// 每列的直方图

    /** Construct a TableStats for an empty table with schema `schema`. */
    public TableStats(Schema schema, int numRecordsPerPage) {
        this.schema = schema;
        this.numRecordsPerPage = numRecordsPerPage;
        this.numRecords = 0;
        this.histograms = new ArrayList<>();
        for (Type t : schema.getFieldTypes()) {
            Histogram h = new Histogram();
            this.histograms.add(h);
        }
    }

    private TableStats(Schema schema, int numRecordsPerPage, int numRecords,
                       List<Histogram> histograms) {
        this.schema = schema;
        this.numRecordsPerPage = numRecordsPerPage;
        this.numRecords = numRecords;
        this.histograms = histograms;
    }

    // Modifiers /////////////////////////////////////////////////////////////////
    public void addRecord(Record record) {
        numRecords++;
    }

    public void removeRecord(Record record) {
        numRecords = Math.max(numRecords - 1, 0);
    }

    public void refreshHistograms(int buckets, Table table) {
        List<Histogram> newHistograms = new ArrayList<>();
        int totalRecords = 0;
        for (int i = 0; i < schema.size(); i++) {
            Histogram h = new Histogram(buckets);
            h.buildHistogram(table, i);
            newHistograms.add(h);
            totalRecords += h.getCount();
        }
        this.histograms = newHistograms;
        this.numRecords = Math.round(((float) totalRecords) / schema.size());
    }

    // Accessors /////////////////////////////////////////////////////////////////
    public Schema getSchema() { return schema; }

    public int getNumRecords() {
        return numRecords;
    }

    /**
     * Calculates the number of data pages required to store `numRecords` records
     * assuming that all records are stored as densely as possible in the pages.
     */
    public int getNumPages() {
        if (numRecords % numRecordsPerPage == 0) return numRecords / numRecordsPerPage;
        return (numRecords / numRecordsPerPage) + 1;
    }

    public List<Histogram> getHistograms() {
        return histograms;
    }

    // Copiers ///////////////////////////////////////////////////////////////////
    /**
     * Estimates the table statistics for the table that would be produced after
     * filtering column `i` with `predicate` and `value`. For simplicity, we
     * assume that columns are completeley uncorrelated. For example, imagine the
     * following table statistics for a table T(x:int, y:int).
     *
     *   numRecords = 100
     *   numPages = 2
     *               Histogram x                         Histogram y
     *               ===========                         ===========
     *   60 |                       50       60 |
     *   50 |        40           +----+     50 |
     *   40 |      +----+         |    |     40 |
     *   30 |      |    |         |    |     30 |   20   20   20   20   20
     *   20 |   10 |    |         |    |     20 | +----+----+----+----+----+
     *   10 | +----+    | 00   00 |    |     10 | |    |    |    |    |    |
     *   00 | |    |    +----+----+    |     00 | |    |    |    |    |    |
     *       ----------------------------        ----------------------------
     *        0    1    2    3    4    5          0    1    2    3    4    5
     *              0    0    0    0    0               0    0    0    0    0
     *
     * If we apply the filter `x < 20`, we estimate that we would have the
     * following table statistics.
     *
     *   numRecords = 50
     *   numPages = 1
     *               Histogram x                         Histogram y
     *               ===========                         ===========
     *   50 |        40                      50 |
     *   40 |      +----+                    40 |
     *   30 |      |    |                    30 |
     *   20 |   10 |    |                    20 |   10   10   10   10   10
     *   10 | +----+    | 00   00   00       10 | +----+----+----+----+----+
     *   00 | |    |    +----+----+----+     00 | |    |    |    |    |    |
     *       ----------------------------        ----------------------------
     *        0    1    2    3    4    5          0    1    2    3    4    5
     *              0    0    0    0    0               0    0    0    0    0
     *
     *  这个方法体现了数据库查询优化中的 列独立性假设 ：
     *
     * - 对于应用谓词的列，精确计算过滤后的分布
     * - 对于其他列，假设它们与目标列独立，按相同比例缩减
     */
    public TableStats copyWithPredicate(int column,
                                        PredicateOperator predicate,
                                        DataBox d) {
        // 计算缩减因子，统计这个谓词操作符predicate结果下数据的缩减因子
        float reductionFactor = histograms.get(column).computeReductionFactor(predicate, d);
        List<Histogram> copyHistograms = new ArrayList<>();
        for (int j = 0; j < histograms.size(); j++) {
            Histogram histogram = histograms.get(j);
            if (column == j) {
                // For the target column, apply the predicate directly
                // 目标列生成新的直方图副本
                copyHistograms.add(histogram.copyWithPredicate(predicate, d));
            } else {
                // For other columns, reduce by the reduction factor
                // 其他列同比例的采用缩减因子reductionFactor缩减
                copyHistograms.add(histogram.copyWithReduction(reductionFactor));
            }
        }
        int numRecords = copyHistograms.get(column).getCount();
        return new TableStats(this.schema, this.numRecordsPerPage, numRecords, copyHistograms);
    }

    /**
     * Creates a new TableStats which is the statistics for the table
     * that results from this TableStats joined with the given TableStats.
     *
     * @param leftIndex the index of the join column for this
     * @param rightStats the TableStats of the right table to be joined
     * @param rightIndex the index of the join column for the right table
     * @return new TableStats based off of this and params
     *
     * 假设：
     *
     * - 左表：1000 条记录，连接列有 100 个不同值
     * - 右表：2000 条记录，连接列有 50 个不同值
     * 计算过程：
     *
     * - 选择性 = 1 / max(100, 50) = 1/100 = 0.01
     * - 输出大小 = 1000 × 2000 × 0.01 = 20,000 条记录
     */
    public TableStats copyWithJoin(int leftIndex,
                                   TableStats rightStats,
                                   int rightIndex) {
        // Compute the new schema.
        // 左右两表的模式合并
        Schema joinedSchema = this.schema.concat(rightStats.schema);
        int inputSize = this.numRecords * rightStats.numRecords;
        int leftNumDistinct = 1;
        if (this.histograms.size() > 0) {
            // 获取右表索引列去重后的个数
            leftNumDistinct = this.histograms.get(leftIndex).getNumDistinct() + 1;
        }

        int rightNumDistinct = 1;
        if (rightStats.histograms.size() > 0) {
            rightNumDistinct = rightStats.histograms.get(rightIndex).getNumDistinct() + 1;
        }

        float reductionFactor = 1.0f / Math.max(leftNumDistinct, rightNumDistinct);
        List<Histogram> copyHistograms = new ArrayList<>();
        // 如果leftNumDistinct最大，则leftReductionFactor为1，否则小于1
        float leftReductionFactor = leftNumDistinct * reductionFactor;
        float rightReductionFactor = rightNumDistinct * reductionFactor;
        int outputSize = (int)(reductionFactor * inputSize);//输出记录数 = 笛卡尔积大小 × 选择性

        for (Histogram leftHistogram : this.histograms) {
            copyHistograms.add(leftHistogram.copyWithJoin(outputSize, leftReductionFactor));
        }

        for (Histogram rightHistogram : rightStats.histograms) {
            copyHistograms.add(rightHistogram.copyWithJoin(outputSize, rightReductionFactor));
        }

        int joinedRecordsPerPage = Table.computeNumRecordsPerPage(
                PageDirectory.EFFECTIVE_PAGE_SIZE, joinedSchema);
        return new TableStats(joinedSchema, joinedRecordsPerPage, outputSize, copyHistograms);
    }
}
