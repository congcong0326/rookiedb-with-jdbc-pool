package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.PageDirectory;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 * Block Nested Loop Join（块嵌套循环连接） 是对 Simple Nested Loop Join（SNLJ） 的性能优化。
 * 它利用了数据库系统中的块（页）级缓存和一次加载多个元组的思想，减少 I/O 次数，从而提高连接性能。
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            // 使用QueryOperator.getBlockIterator方法获取一个块的记录
            // numBuffers-2表示可用于左表的缓冲区数量（总缓冲区减去输出缓冲区和右表缓冲区）
            if (leftSourceIterator.hasNext()) {
                leftBlockIterator = QueryOperator.getBlockIterator(
                        leftSourceIterator,
                        getLeftSource().getSchema(),
                        numBuffers - 2
                );
                leftBlockIterator.markNext();
                leftRecord = leftBlockIterator.next();
            }
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            if (rightSourceIterator.hasNext()) {
                rightPageIterator = QueryOperator.getBlockIterator(
                        rightSourceIterator,
                        getRightSource().getSchema(),
                        1
                );
                rightPageIterator.markNext();
            }
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            if (leftRecord == null) {
                // The left source was empty, nothing to fetch
                return null;
            }
            // 先拿第一个block的第一个record和第一个page join
            // 如果第一个page用完，还没匹配上，则用一个block的下一个page
            // 如果第一个block用完，则更新下一页page
            // 如果所有right扫描完，则更新下一个block，此时可以
            while (true) {
                //情况 1：右侧页面迭代器有一个值可以生成
                if (rightPageIterator.hasNext()) {
                    Record rightRecord = rightPageIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                }
                //情况 2：右侧页面迭代器没有值可以生成，但左侧块迭代器有
                else if (leftBlockIterator.hasNext()) {
                    leftRecord = leftBlockIterator.next();
                    rightPageIterator.reset();
                }
                //情况 3：右侧页面和左侧块迭代器都没有值可以生成，但右侧还有更多页面
                else if (rightSourceIterator.hasNext()) {
                    leftBlockIterator.reset();
                    leftRecord = leftBlockIterator.next();
                    fetchNextRightPage();
                }
                //情况 4：右侧页面和左侧块迭代器都没有值可以生成，且右侧也没有更多页面，但左侧还有块
                else if (leftSourceIterator.hasNext()) {
                    fetchNextLeftBlock();
                    rightSourceIterator.reset();
                    fetchNextRightPage();
                } else {
                    return null;
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
