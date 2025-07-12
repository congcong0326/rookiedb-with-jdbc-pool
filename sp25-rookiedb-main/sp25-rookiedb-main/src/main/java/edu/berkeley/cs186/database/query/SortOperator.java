package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     * 内部排序，将数据全部读取到内存
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        // 使用临时表来保存数据，run 持久化数据到了磁盘，适合于外部排序
        Run run = new Run(transaction, getSchema());
        List<Record> sortList = new ArrayList<>();
        // 这里允许将所有数据在内存排序，说明给定的 records 的数据总量是可控的
        while (records.hasNext()) {
            Record record = records.next();
            sortList.add(record);
        }
        sortList.sort(new RecordComparator());
        run.addAll(sortList);
        return run;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     * 多路归并归并排序的列表，这是一个外部排序
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement
        PriorityQueue<Pair<Record, Integer>> queue = new PriorityQueue<>(runs.size(), new RecordPairComparator());
        List<BacktrackingIterator<Record>> mergeTracker = new ArrayList<>();
        // iterator需要保存他们的引用，不然每次获取到的不一致
        assert runs.get(0).iterator() != runs.get(0).iterator();
        for (Run run : runs) {
            mergeTracker.add(run.iterator());
        }
        for (int i = 0; i < mergeTracker.size(); i++) {
            if (mergeTracker.get(i).hasNext()) {
                queue.add(new Pair<>(mergeTracker.get(i).next(), i));
            }
        }
        // 再次创建一个临时表
        Run run = new Run(transaction, getSchema());

        while (!queue.isEmpty()) {
            Pair<Record, Integer> poll = queue.poll();
            if (poll != null) {
                // 将排序的数据转移到临时表
                run.add(poll.getFirst());
                // 需要补充下一个元素
                Integer index = poll.getSecond();
                // 有可能候选已经没有数据了
                if (mergeTracker.get(index).hasNext()) {
                    queue.add(new Pair<>(mergeTracker.get(index).next(), index));
                }
            }
        }
        return run;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     * @return a list of sorted runs obtained by merging the input runs
     * 给定N个已经排序的外部临时表，把他们merge到一起，也就是执行K路归并
     * K要求最大为 numBuffers - 1
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        assert numBuffers - 1 > 1;
        List<Run> result = new ArrayList<>();
        List<Run> tmp = new ArrayList<>(numBuffers - 1);
        for (Run run : runs) {
            tmp.add(run);
            if (tmp.size() >= numBuffers - 1) {
                result.add(mergeSortedRuns(tmp));
                tmp.clear();
            }
        }
        if (!tmp.isEmpty()) {
            // 一个元素就不需要合并了
            if (tmp.size() == 1) {
                result.add(tmp.get(0));
            } else {
                result.add(mergeSortedRuns(tmp));
            }
        }
        return result;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // TODO(proj3_part1): implement
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();

        List<Run> tmpTableList = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            // 采用 numBuffers 大小的页面对数据迭代
            BacktrackingIterator<Record> blockIterator = getBlockIterator(sourceIterator, getSchema(), numBuffers);
            // 创建临时表
            Run tmpTable = sortRun(blockIterator);
            tmpTableList.add(tmpTable);
        }
        // 得到一堆外排序好的临时表，需要对他们进行归并
        // 此时还不能调用这个，因为有内存限制
        // Run mergeResult = mergeSortedRuns(tmpTableList);
        // 多次调用 mergePass 分批次对数据进行排序，merge的输出只有一个临时表则意味着合并完成
        List<Run> runs = mergePass(tmpTableList);
        while (runs.size() > 1) {
            runs = mergePass(runs);
        }

        return runs.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

