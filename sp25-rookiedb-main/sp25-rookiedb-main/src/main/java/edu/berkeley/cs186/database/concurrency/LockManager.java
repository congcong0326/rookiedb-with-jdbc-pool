package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.recovery.LogType;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    // 一个从事务ID到该事务持有的锁列表的映射
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    // 个从资源名称到资源条目的映射，每个资源条目包含该资源上的锁列表和等待队列。
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         * 检查锁是否和之前应存在的锁冲突，但是可以跳过指定事物的锁
         * 需要迭代资源上的锁，判断是否和传入的lockType兼容
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            // 遍历当前资源上的所有锁
            for (Lock lock : locks) {
                // 跳过指定事务的锁
                if (lock.transactionNum == except) {
                    continue;
                }
                // 检查请求的锁类型与当前锁是否兼容
                if (!LockType.compatible(lockType, lock.lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock addLock) {
            // TODO(proj4_part1): implement
            // 查找同一事务是否已持有该资源的锁
            boolean sameTransactionNum = false;
            for (int i = 0; i < locks.size(); i++) {
                Lock lock = locks.get(i);
                if (Objects.equals(lock.transactionNum, addLock.transactionNum)) {
                    // 更新锁类型
                    sameTransactionNum = true;
                    locks.set(i, addLock);
                }
            }
            // 如果没有找到同一事务的锁，添加新锁
            if (!sameTransactionNum) {
                locks.add(addLock);
            }
            List<Lock> transactionLocksList = transactionLocks.getOrDefault(addLock.transactionNum, new ArrayList<>());
            transactionLocksList.add(addLock);
            transactionLocks.put(addLock.transactionNum, transactionLocksList);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock releasedLock) {
            // TODO(proj4_part1): implement
            locks.remove(releasedLock);
            //
            List<Lock> transactionLocksList = transactionLocks.getOrDefault(releasedLock.transactionNum, new ArrayList<>());
            if (!transactionLocksList.isEmpty()) {
                transactionLocksList.remove(releasedLock);
            }
            // 处理等待队列中的请求
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.add(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            while (requests.hasNext()) {
                LockRequest request = requests.next();
                Lock lock = request.lock;
                // 兼容
                if (checkCompatible(lock.lockType, request.transaction.getTransNum())) {
                    requests.remove();
                    List<Lock> releasedLocks = request.releasedLocks;
                    for (Lock releasedLock : releasedLocks) {
                        releaseLock(releasedLock);
                    }
                    grantOrUpdateLock(lock);
                    request.transaction.unblock();
                } else {
                    break;
                }
            }
            // TODO(proj4_part1): implement
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         * 该方法假设一个事务在一个资源上最多持有一个锁?
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (Objects.equals(lock.transactionNum, transaction)) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     * 获取传入的name锁，释放传入的releaseNames锁，并且这两个是一个原子操作，也就是放在同步代码中完成
     *
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     * 如果锁不兼容其他事物的锁，需要阻塞该事物，并且放到队列前面
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * 该方法不修改获取锁的时间
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     *  检查事物是否已经持有对name的锁，如果是则抛出异常
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     * 检查事物是否持有releaseNames中的锁，没有则抛出异常
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            // 获取事物ID
            shouldBlock = doAcquireAndReleaseReturnNeedBlock(transaction, name, lockType, releaseNames, true);
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    private boolean doAcquireAndReleaseReturnNeedBlock(TransactionContext transaction, ResourceName name,
                                     LockType lockType, List<ResourceName> releaseNames, boolean addFront) {
        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();
        // 获取资源
        ResourceEntry resourceEntry = getResourceEntry(name);
        // 需要加锁，需要检测该事物是否已经持有需要加的锁
        // 检查事物是否已经持有对name的锁，并且该锁没打算释放，如果是则抛出异常
        Lock existingLock = null;
        for (Lock lock : resourceEntry.locks) {
            if (lock.transactionNum == transNum) {
                existingLock = lock;
                break;
            }
        }
        //&& !releaseNames.contains(name)
        if (existingLock != null && !releaseNames.contains(name)) {
            throw new DuplicateLockRequestException("`transaction` doesn't hold a lock on one or more of the names in `releaseNames`");
        }
        //
        List<Lock> releasedLocks = new ArrayList<>();
        for (ResourceName releaseName : releaseNames) {
            ResourceEntry needResourceEntry = getResourceEntry(releaseName);
            Lock releaseLock = null;

            for (Lock lock : needResourceEntry.locks) {
                if (lock.transactionNum == transNum) {
                    releaseLock = lock;
                    releasedLocks.add(lock);
                }
            }

            if (releaseLock == null) {
                throw new NoLockHeldException("事务未持有资源的锁: " + releaseName);
            }
        }
        // 创建锁对象
        Lock newLock = new Lock(name, lockType, transNum);
        // 检查是否可以立即获取锁
        if (resourceEntry.waitingQueue.isEmpty() && resourceEntry.checkCompatible(newLock.lockType, transNum)) {
           // 释放所有锁
            for (Lock releasedLock : releasedLocks) {
                getResourceEntry(releasedLock.name).releaseLock(releasedLock);
            }
            //获取锁
            resourceEntry.grantOrUpdateLock(newLock);
        } else {
            // 需要阻塞，创建锁请求并添加到等待队列
            LockRequest request = new LockRequest(transaction, newLock, releasedLocks);
            resourceEntry.addToQueue(request, addFront);
            shouldBlock = true;
            transaction.prepareBlock();
        }
        return shouldBlock;
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            shouldBlock = doAcquireAndReleaseReturnNeedBlock(transaction, name, lockType, Collections.emptyList(), false);
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            long transNum = transaction.getTransNum();
            ResourceEntry resourceEntry = getResourceEntry(name);

            List<Lock> releaseLocks = new ArrayList<>();
            for (Lock lock : resourceEntry.locks) {
                if (lock.transactionNum == transNum) {
                    releaseLocks.add(lock);
                }
            }
            if (releaseLocks.isEmpty()) {
                throw new NoLockHeldException("事务未持有资源的锁: " + name);
            }
            for (Lock releaseLock : releaseLocks) {
                resourceEntry.releaseLock(releaseLock);
            }
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            ResourceEntry resourceEntry = getResourceEntry(name);
            Lock oldLock = null;

            // 1. 查找事务持有的旧锁
            for (Lock lock : resourceEntry.locks) {
                if (transNum == lock.transactionNum) {
                    oldLock = lock;
                    break; // 找到旧锁后立即退出循环
                }
            }

            // 2. 检查是否持有锁
            if (oldLock == null) {
                throw new NoLockHeldException("`transaction` has no lock on `name` " + name);
            }

            // 3. 检查是否已经持有相同类型的锁
            if (Objects.equals(oldLock.lockType, newLockType)) {
                throw new DuplicateLockRequestException("`transaction` already has a newLockType");
            }

            // 4. 检查锁类型是否可替代
            if (!LockType.substitutable(newLockType, oldLock.lockType)) {
                throw new InvalidLockException(oldLock.lockType + " old lock type is not substitutable by new lock " + newLockType);
            }

            // 5. 检查与其他事务锁的兼容性
            for (Lock lock : resourceEntry.locks) {
                if (lock.transactionNum != transNum && !LockType.compatible(lock.lockType, newLockType)) {
                    // 需要阻塞，创建锁请求并添加到等待队列
                    Lock newLock = new Lock(name, newLockType, transNum);
                    List<Lock> releaseLocks = new ArrayList<>();
                    releaseLocks.add(oldLock);
                    LockRequest request = new LockRequest(transaction, newLock, releaseLocks);
                    resourceEntry.addToQueue(request, true); // 添加到队列前面
                    shouldBlock = true;
                    transaction.prepareBlock();
                    break; // 发现不兼容立即退出循环
                }
            }

            // 6. 如果不需要阻塞，直接升级锁
            if (!shouldBlock) {
                // 释放旧锁并授予新锁
                resourceEntry.releaseLock(oldLock);
                resourceEntry.grantOrUpdateLock(new Lock(name, newLockType, transNum));
                //resourceEntry
                //acquireAndRelease(transaction, name, newLockType, Collections.singletonList(name));
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        long transNum = transaction.getTransNum();
        for (Lock lock : resourceEntry.locks) {
            if (lock.transactionNum == transNum) {
                return lock.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
