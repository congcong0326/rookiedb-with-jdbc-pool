package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static edu.berkeley.cs186.database.recovery.LogType.*;
import static edu.berkeley.cs186.database.recovery.records.EndCheckpointLogRecord.fitsInOneRecord;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     *
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        long lastLSN = transactionTableEntry.lastLSN;
        // 写入commit日志
        CommitTransactionLogRecord commitTransactionLogRecord = new CommitTransactionLogRecord(transNum, lastLSN);
        long LSN = logManager.appendToLog(commitTransactionLogRecord);
        // 预写日志机制，写入成功后再更新状态
        // 根据ARIES协议，当一个事务提交时，它的 COMMIT 日志记录以及之前所有的日志记录都必须被强制写入（flush）到稳定存储中。
        // 这确保了事务的持久性（Durability）。只有当日志被成功刷新后，事务才能被认为是真正提交了。这里的实现是符合规范的。
        logManager.flushToLSN(LSN);
        transactionTableEntry.lastLSN = LSN;
        transactionTableEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        // abort 只写入一个abort日志，在commit阶段进程撤销操作
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        long lastLSN = transactionTableEntry.lastLSN;
        AbortTransactionLogRecord abortTransactionLogRecord = new AbortTransactionLogRecord(transNum, lastLSN);
        long LSN = logManager.appendToLog(abortTransactionLogRecord);
        // 预写日志机制，写入成功后再更新状态
        // 与 commit 类似，当一个事务决定中止时，记录这个决定的 ABORT 日志记录也需要被持久化。
        // 这确保了即使在故障恢复后，系统也知道这个事务最终的状态是中止，从而可以正确地执行回滚操作。这里的实现是正确的。
        logManager.flushToLSN(LSN);
        transactionTableEntry.lastLSN = LSN;
        transactionTableEntry.transaction.setStatus(Transaction.Status.ABORTING);
        return LSN;
    }



    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        Transaction.Status status = transactionTableEntry.transaction.getStatus();
        if (status == Transaction.Status.ABORTING) {
            // 撤销事物的相关操作
            rollbackToLSN(transNum, 0L);
        }
        long lastLSN = transactionTableEntry.lastLSN;
        EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(transNum, lastLSN);
        long LSN = logManager.appendToLog(endTransactionLogRecord);
        // END 记录标志着一个事务（无论是提交还是中止）的清理工作已经完成。
        // 将 END 记录刷新到磁盘，可以确保在下一次恢复时，系统知道这个事务已经完全结束，不需要再为它做任何恢复操作。这对于垃圾回收和优化恢复过程很重要。
        logManager.flushToLSN(LSN);
        //transactionTableEntry.transaction.cleanup();
        transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.remove(transNum);
        return LSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        // 当前事物ID比传入LSN大
            // 如果当前record是undoable
            // 生成补偿日志clr
            // 写入日志
            // 重做clr
        // 更新下一个需要撤销的record
        // TODO(proj5) implement the rollback logic described above
        while (lastRecord.getUndoNextLSN()
                .orElse(lastRecord.getLSN()) > LSN) {
            // UndoUpdatePageLogRecord，UndoAllocPageLogRecord 两个类型的clr日志 返回 false
            // 所以CLR是补偿日志，如果redo过一次是不会再次执行的

            if (lastRecord.isUndoable()) {
                // 新的CLR preLSN需要指向事物最后一个提交的LSN，形成链式结构
                LogRecord CLR = lastRecord.undo(transactionEntry.lastLSN);
                long CLR_LSN = logManager.appendToLog(CLR);
                // 在回滚过程中，每生成一个补偿日志记录（CLR），它都描述了一个已经完成的“撤销”操作。
                // 根据WAL协议，这个描述“撤销”的CLR日志必须在对应的页面修改（即 CLR.redo() ）写入磁盘之前被持久化。
                // 由于 CLR.redo() 会修改页面，所以在此之前刷新CLR日志是至关重要的，确保了撤销操作本身也是可恢复的。
                logManager.flushToLSN(CLR_LSN);
                CLR.redo(this, diskSpaceManager, bufferManager);
                transactionEntry.lastLSN = CLR_LSN;
                // 链式迭代上一个record
                if (lastRecord.getUndoNextLSN().isPresent()) {
                    lastRecord = logManager.fetchLogRecord(lastRecord.getUndoNextLSN().get());
                } else if (lastRecord.getPrevLSN().isPresent()) {
                    lastRecord = logManager.fetchLogRecord(lastRecord.getPrevLSN().get());
                } else {
                    break;
                }
            }
            else {
                if (lastRecord.getUndoNextLSN().isPresent()) {
                    lastRecord = logManager.fetchLogRecord(lastRecord.getUndoNextLSN().get());
                } else if (lastRecord.getPrevLSN().isPresent()) {
                    lastRecord = logManager.fetchLogRecord(lastRecord.getPrevLSN().get());
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long lastLSN = transactionEntry.lastLSN;
        UpdatePageLogRecord updatePageLogRecord = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, after);
        long LSN = logManager.appendToLog(updatePageLogRecord);
        /**
         * 根据WAL协议，对页面的任何修改都必须在修改实际写入磁盘 之前 ，先将描述该修改的日志记录持久化到稳定存储中。
         * 当前的代码在将日志记录写入日志后，立即调用了 logManager.flushToLSN(LSN) ，这强制将日志刷新到磁盘。
         * 这对于 commit 等操作是正确的，但对于普通的页面写入操作来说，过于频繁的刷新会严重影响性能。
         */
        //logManager.flushToLSN(LSN);
        transactionEntry.lastLSN = LSN;

        dirtyPage(pageNum, LSN);

        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);
        // TODO(proj5): implement
        rollbackToLSN(transNum, savepointLSN);
        releaseSavepoint(transNum, name);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, chkptTxnTable.size())) {
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);
                chkptDPT.clear();
            }
            chkptDPT.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);
                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            chkptTxnTable.put(entry.getKey(), new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        long lastLSN = logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(lastLSN);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> iterator = logManager.scanFrom(LSN);
        while (iterator.hasNext()) {
            LogRecord logRecord = iterator.next();
            LogType type = logRecord.getType();
            Optional<Long> transNumOpt = logRecord.getTransNum();

            // Create a transaction table entry if one doesn't exist and the transaction hasn't ended.
            if (transNumOpt.isPresent()) {
                long txNum = transNumOpt.get();
                if (!endedTransactions.contains(txNum)) {
                    // putIfAbsent别用，会导致 newTransaction.apply 的提前执行
                    if (!transactionTable.containsKey(txNum)) {
                        transactionTable.put(txNum, new TransactionTableEntry(newTransaction.apply(txNum)));
                    }
                }
                // Update lastLSN for the transaction.
                if (transactionTable.containsKey(txNum)) {
                    transactionTable.get(txNum).lastLSN = logRecord.LSN;
                }
            }

            switch (type) {
                case END_CHECKPOINT: {
                    dirtyPageTable.putAll(((EndCheckpointLogRecord) logRecord).getDirtyPageTable());
                    Map<Long, Pair<Transaction.Status, Long>> checkpointTxnTable = ((EndCheckpointLogRecord) logRecord).getTransactionTable();
                    for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : checkpointTxnTable.entrySet()) {
                        long txNum = entry.getKey();
                        Transaction.Status status = entry.getValue().getFirst();
                        long lastLSN = entry.getValue().getSecond();
                        if (endedTransactions.contains(txNum)) continue;
                        if (!transactionTable.containsKey(txNum)) {
                            transactionTable.put(txNum, new TransactionTableEntry(newTransaction.apply(txNum)));
                        }
                        TransactionTableEntry txEntry = transactionTable.get(txNum);

                        if (lastLSN > txEntry.lastLSN) {
                            txEntry.lastLSN = lastLSN;
                        }
                        if (status == Transaction.Status.ABORTING) {
                            txEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        } else if (status == Transaction.Status.COMMITTING) {
                            txEntry.transaction.setStatus(Transaction.Status.COMMITTING);
                        }
                    }
                    break;
                }
                case COMMIT_TRANSACTION:
                    transNumOpt.ifPresent(txNum -> transactionTable.get(txNum).transaction.setStatus(Transaction.Status.COMMITTING));
                    break;
                case ABORT_TRANSACTION:
                    transNumOpt.ifPresent(txNum -> transactionTable.get(txNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING));
                    break;
                case END_TRANSACTION: {
                    transNumOpt.ifPresent(txNum -> {
                        TransactionTableEntry remove = transactionTable.remove(txNum);
                        if (remove != null) {
                            remove.transaction.cleanup();
                            remove.transaction.setStatus(Transaction.Status.COMPLETE);
                        }
                        endedTransactions.add(txNum);
                    });
                    break;
                }
                case ALLOC_PAGE:
                case UPDATE_PAGE:
                case UNDO_UPDATE_PAGE: {
                    logRecord.getPageNum().ifPresent(pageNum -> dirtyPageTable.putIfAbsent(pageNum, logRecord.LSN));
                    break;
                }
                case FREE_PAGE:
                case UNDO_ALLOC_PAGE: {
                    logRecord.getPageNum().ifPresent(dirtyPageTable::remove);
                    break;
                }
            }
        }

        Set<Long> committingTransactions = new HashSet<>();
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long txNum = entry.getKey();
            TransactionTableEntry txEntry = entry.getValue();
            if (txEntry.transaction.getStatus() == Transaction.Status.COMMITTING) {
                committingTransactions.add(txNum);
            } else if (txEntry.transaction.getStatus() == Transaction.Status.RUNNING) {
                txEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                AbortTransactionLogRecord abortRecord = new AbortTransactionLogRecord(txNum, txEntry.lastLSN);
                txEntry.lastLSN = logManager.appendToLog(abortRecord);
            }
        }

        for (long txNum : committingTransactions) {
            //clean up the transaction, change status to COMPLETE,
            //     *    remove from the ttable, and append an end record
            TransactionTableEntry removeTx = transactionTable.remove(txNum);
            if (removeTx != null) {
                removeTx.transaction.cleanup();
                EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(removeTx.transaction.getTransNum(), removeTx.lastLSN);
                logManager.appendToLog(endTransactionLogRecord);
            }
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        if (dirtyPageTable.isEmpty()) {
            return;
        }
        long minLSN = Long.MAX_VALUE;
        for (Long value : dirtyPageTable.values()) {
            minLSN = Math.min(value, minLSN);
        }
        Iterator<LogRecord> iterator = logManager.scanFrom(minLSN);
        while (iterator.hasNext()) {
            LogRecord logRecord = iterator.next();
            LogType type = logRecord.getType();

            if (logRecord.isRedoable()) {
                // 1. partition-related: 总是重做
                if (type.toString().contains("PART")) {
                    logRecord.redo(this, diskSpaceManager, bufferManager);
                }
                // 2. allocates a page: 总是重做
                else if (type == LogType.ALLOC_PAGE || type == LogType.UNDO_FREE_PAGE) {
                    logRecord.redo(this, diskSpaceManager, bufferManager);
                }
                // 3. modifies a page: 需要检查脏页表和 recLSN
                else if (type == LogType.UPDATE_PAGE || type == LogType.UNDO_UPDATE_PAGE ||
                        type == LogType.FREE_PAGE || type == LogType.UNDO_ALLOC_PAGE) {
                    Optional<Long> pageNum = logRecord.getPageNum();
                    if (pageNum.isPresent()) {
                        Long recLSN = dirtyPageTable.get(pageNum.get());
                        // 页面在脏页表中且日志 LSN >= recLSN
                        if (recLSN != null && logRecord.getLSN() >= recLSN) {
                            Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum.get());
                            try {
                                // 磁盘页面的LSN需要小于日志的LSN，如果 pageLSN ≥ 记录的 LSN，说明这个修改已经被应用了，不需要重做
                                if (page.getPageLSN() < logRecord.getLSN()) {
                                    logRecord.redo(this, diskSpaceManager, bufferManager);
                                }
                            } finally {
                                page.unpin();
                            }
                        }
                    }
                }
            }
        }
        return;
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<LogRecord> priorityQueue = new PriorityQueue<>(new Comparator<LogRecord>() {
            @Override
            public int compare(LogRecord o1, LogRecord o2) {
                return Long.compare(o2.getLSN(), o1.getLSN());
            }
        });
        for (TransactionTableEntry transactionTableEntry : transactionTable.values()) {
            if (transactionTableEntry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                priorityQueue.add(logManager.fetchLogRecord(transactionTableEntry.lastLSN));
            }
        }
        // 使用优先级队列是为了避免过多的随机IO，并且在一趟扫描撤销所有
        while (!priorityQueue.isEmpty()) {
            LogRecord logRecord = priorityQueue.poll();
            if (logRecord.getTransNum().isPresent()) {
                Long txNum = logRecord.getTransNum().get();
                TransactionTableEntry transactionEntry = transactionTable.get(txNum);
                if (logRecord.isUndoable()) {
                    LogRecord CLR = logRecord.undo(transactionEntry.lastLSN);
                    long CLR_LSN = logManager.appendToLog(CLR);
                    // 1. ARIES算法中，CLR的持久化时机由页面修改的WAL协议决定，不是在生成时立即刷新
                    //logManager.flushToLSN(CLR_LSN);  // 先刷新
                    transactionEntry.lastLSN = CLR_LSN;  // 再更新lastLSN
                    CLR.redo(this, diskSpaceManager, bufferManager);
                }
                long nextLSN = 0;
                if (logRecord.getUndoNextLSN().isPresent()) {
                    nextLSN = logRecord.getUndoNextLSN().get();
                } else if (logRecord.getPrevLSN().isPresent()) {
                    nextLSN = logRecord.getPrevLSN().get();
                }
                if (nextLSN == 0) {
                    long lastLSN = transactionEntry.lastLSN;
                    EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(txNum, lastLSN);
                    long LSN = logManager.appendToLog(endTransactionLogRecord);
                    // END 记录标志着一个事务（无论是提交还是中止）的清理工作已经完成。
                    logManager.flushToLSN(LSN);
                    transactionEntry.transaction.cleanup();
                    transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(txNum);
                } else {
                    priorityQueue.add(logManager.fetchLogRecord(nextLSN));
                }
            }
        }
        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
