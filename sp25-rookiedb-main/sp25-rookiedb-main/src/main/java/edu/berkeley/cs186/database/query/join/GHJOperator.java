package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.HashFunc;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.disk.Partition;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

public class GHJOperator extends JoinOperator {
    private int numBuffers;
    private Run joinedRecords;

    public GHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.GHJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
        this.joinedRecords = null;
    }

    @Override
    public int estimateIOCost() {
        // Since this has a chance of failing on certain inputs we give it the
        // maximum possible cost to encourage the optimizer to avoid it
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (joinedRecords == null) {
            // Executing GHJ on-the-fly is arduous without coroutines, so
            // instead we'll accumulate all of our joined records in this run
            // and return an iterator over it once the algorithm completes
            this.joinedRecords = new Run(getTransaction(), getSchema());
            this.run(getLeftSource(), getRightSource(), 1);
        };
        return joinedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * For every record in the given iterator, hashes the value
     * at the column we're joining on and adds it to the correct partition in
     * partitions.
     *
     * @param partitions an array of partitions to split the records into
     * @param records iterable of records we want to partition
     * @param left true if records are from the left relation, otherwise false
     * @param pass the current pass (used to pick a hash function)
     */
    private void partition(Partition[] partitions, Iterable<Record> records, boolean left, int pass) {
        // TODO(proj3_part1): implement the partitioning logic
        // You may find the implementation in SHJOperator.java to be a good
        // starting point. You can use the static method HashFunc.hashDataBox
        // to get a hash value.
        for (Record record : records) {
            DataBox columnValue = record.getValue(left ? getLeftColumnIndex() : getRightColumnIndex());
            int hash = HashFunc.hashDataBox(columnValue, pass);
            int partitionNum = hash % partitions.length;
            if (partitionNum < 0)  // hash might be negative
                partitionNum += partitions.length;
            partitions[partitionNum].add(record);
        }
        return;
    }

    /**
     * Runs the buildAndProbe stage on a given partition. Should add any
     * matching records found during the probing stage to this.joinedRecords.
     * - SHJ（Simple Hash Join）只对左表进行分区，然后用整个右表探测
     * - GHJ（Grace Hash Join）对左右表都进行分区，然后在对应的分区之间执行连接
     * - GHJ更适合处理大数据集，因为它可以递归地将大分区进一步分解
     */
    private void buildAndProbe(Partition leftPartition, Partition rightPartition) {
        // true if the probe records come from the left partition, false otherwise
        boolean probeFirst;
        // We'll build our in memory hash table with these records
        Iterable<Record> buildRecords;
        // We'll probe the table with these records
        Iterable<Record> probeRecords;
        // The index of the join column for the build records
        int buildColumnIndex;
        // The index of the join column for the probe records
        int probeColumnIndex;
        // 方法首先检查左右分区的大小，选择较小的分区作为构建哈希表的数据源（buildRecords）
        // 这是因为构建哈希表需要将整个分区加载到内存中，所以选择较小的分区可以减少内存使用
        // 如果左分区足够小（页数 ≤ numBuffers-2），则使用左分区构建哈希表
        if (leftPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = leftPartition;
            buildColumnIndex = getLeftColumnIndex();
            probeRecords = rightPartition;
            probeColumnIndex = getRightColumnIndex();
            probeFirst = false;
        }
        // 如果右分区足够小，则使用右分区构建哈希表
        else if (rightPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = rightPartition;
            buildColumnIndex = getRightColumnIndex();
            probeRecords = leftPartition;
            probeColumnIndex = getLeftColumnIndex();
            probeFirst = true;
        }
        // 如果两个分区都太大，则抛出异常，表示需要进一步分区
        else {
            throw new IllegalArgumentException(
                "Neither the left nor the right records in this partition " +
                "fit in B-2 pages of memory."
            );
        }
        // TODO(proj3_part1): implement the building and probing stage
        // You shouldn't refer to any variable starting with "left" or "right"
        // here, use the "build" and "probe" variables we set up for you.
        // Check out how SHJOperator implements this function if you feel stuck.
        Map<DataBox, List<Record>> hashTable = new HashMap<>();
        // 遍历buildRecords中的每条记录
        // 提取连接列的值作为哈希表的键
        for (Record buildRecord : buildRecords) {
            DataBox value = buildRecord.getValue(buildColumnIndex);
            if (!hashTable.containsKey(value)) {
                hashTable.put(value, new ArrayList<>());
            }
            // 将记录添加到对应键的记录列表中
            // 样就构建了一个从连接值到记录列表的映射
            hashTable.get(value).add(buildRecord);
        }
        // 遍历probeRecords中的每条记录
        for (Record probeRecord : probeRecords) {
            // 提取连接列的值，在哈希表中查找匹配项
            DataBox value = probeRecord.getValue(probeColumnIndex);
            if (!hashTable.containsKey(value)) continue;
            // 如果找到匹配项，则将探测记录与哈希表中的每条匹配记录连接
            for (Record buildRecord : hashTable.get(value)) {
                Record grace;
                // 根据probeFirst标志决定连接顺序，确保左表记录在前，右表记录在后
                if (probeFirst) {
                    // buildRecord 是右表
                    grace = probeRecord.concat(buildRecord);
                } else {
                    // buildRecord 是左表
                    grace = buildRecord.concat(probeRecord);
                }
                this.joinedRecords.add(grace);
            }
        }
    }

    /**
     * Runs the grace hash join algorithm. Each pass starts by partitioning
     * leftRecords and rightRecords. If we can run build and probe on a
     * partition we should immediately do so, otherwise we should apply the
     * grace hash join algorithm recursively to break up the partitions further.
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) throw new IllegalStateException("Reached the max number of passes");

        // Create empty partitions
        Partition[] leftPartitions = createPartitions(true);
        Partition[] rightPartitions = createPartitions(false);

        // Partition records into left and right
        // 使用hash函数对所有键进行分区操作
        this.partition(leftPartitions, leftRecords, true, pass);
        this.partition(rightPartitions, rightRecords, false, pass);

        for (int i = 0; i < leftPartitions.length; i++) {
            // TODO(proj3_part1): implement the rest of grace hash join
            // If you meet the conditions to run the build and probe you should
            // do so immediately. Otherwise you should make a recursive call.
            Partition leftPartition = leftPartitions[i];
            Partition rightPartition = rightPartitions[i];
            // 判断某个分区是否小得可以直接放到内存当中
            if (leftPartition.getNumPages() <= numBuffers - 2
                    || rightPartition.getNumPages() <= numBuffers - 2) {
                buildAndProbe(leftPartition, rightPartition);
            } else {
                //如果不行，采用其他hash函数继续进行分区操作
                run(leftPartition, rightPartition, pass + 1);
            }
        }
    }

    // Provided Helpers ////////////////////////////////////////////////////////

    /**
     * Create an appropriate number of partitions relative to the number of
     * available buffers we have.
     *
     * @return an array of partitions
     */
    private Partition[] createPartitions(boolean left) {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            partitions[i] = createPartition(left);
        }
        return partitions;
    }

    /**
     * Creates either a regular partition or a smart partition depending on the
     * value of this.useSmartPartition.
     * @param left true if this partition will store records from the left
     *             relation, false otherwise
     * @return a partition to store records from the specified partition
     */
    private Partition createPartition(boolean left) {
        Schema schema = getRightSource().getSchema();
        if (left) schema = getLeftSource().getSchema();
        return new Partition(getTransaction(), schema);
    }

    // Student Input Methods ///////////////////////////////////////////////////

    /**
     * Creates a record using val as the value for a single column of type int.
     * An extra column containing a 500 byte string is appended so that each
     * page will hold exactly 8 records.
     *
     * @param val value the field will take
     * @return a record
     */
    private static Record createRecord(int val) {
        String s = new String(new char[500]);
        return new Record(val, s);
    }

    /**
     * This method is called in testBreakSHJButPassGHJ.
     *
     * Come up with two lists of records for leftRecords and rightRecords such
     * that SHJ will error when given those relations, but GHJ will successfully
     * run. createRecord(int val) takes in an integer value and returns a record
     * with that value in the column being joined on.
     *
     * Hints: Both joins will have access to B=6 buffers and each page can fit
     * exactly 8 records.
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakSHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();

        // TODO(proj3_part1): populate leftRecords and rightRecords such that
        // SHJ breaks when trying to join them but not GHJ
        for (int i = 0; i < 161; i++) {
            leftRecords.add(createRecord(i));
            rightRecords.add(createRecord(i));
        }
        return new Pair<>(leftRecords, rightRecords);
    }

    /**
     * This method is called in testGHJBreak.
     *
     * Come up with two lists of records for leftRecords and rightRecords such
     * that GHJ will error (in our case hit the maximum number of passes).
     * createRecord(int val) takes in an integer value and returns a record
     * with that value in the column being joined on.
     *
     * Hints: Both joins will have access to B=6 buffers and each page can fit
     * exactly 8 records.
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakGHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();
        // TODO(proj3_part1): populate leftRecords and rightRecords such that GHJ breaks

        for (int i = 0; i < 33; i++) {
            leftRecords.add(createRecord(0));
            rightRecords.add(createRecord(0));
        }
        return new Pair<>(leftRecords, rightRecords);
    }
}

