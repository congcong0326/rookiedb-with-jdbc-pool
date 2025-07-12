package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        // 检查LockContext是否为只读
        if (readonly) {
            throw new UnsupportedOperationException("LockContext is readonly");
        }

        // 检查是否尝试获取NL锁
        if (lockType == LockType.NL) {
            throw new InvalidLockException("Cannot acquire NL lock, use release instead");
        }

        // 需要检查锁的兼容性，父子关系是否兼容
        if (parent != null) {
            LockType parentLockType = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException("Parent lock type " + parentLockType +
                        " is not compatible with child lock type " + lockType);
            }
        }

        // 调用底层获取锁
        lockman.acquire(transaction, name, lockType);
        if (parent != null) {
            parent.numChildLocks.compute(transaction.getTransNum(), (k, v) -> v == null ? 1 : v + 1);
        }
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implemen
        // 检查LockContext是否为只读
        if (readonly) {
            throw new UnsupportedOperationException("LockContext is readonly");
        }

        // 获取当前事务在该资源上持有的显示的锁
        LockType lockType = getExplicitLockType(transaction);
        if (lockType == LockType.NL) {
            throw new NoLockHeldException("No lock held on " + name + " by " + transaction.getTransNum());
        }

        // 检查是否有子资源持有锁，如果有则不能释放当前锁（多粒度锁定约束）
        int numChildren = getNumChildren(transaction);
        if (numChildren > 0) {
            throw new InvalidLockException("Cannot release lock when children resources hold locks");
        }

        // 调用LockManager的release方法释放锁
        lockman.release(transaction, name);

        // 如果有父上下文，更新父上下文的numChildLocks计数
        if (parent != null) {
            parent.numChildLocks.compute(transaction.getTransNum(), (k, v) -> v - 1);
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     *
     *
     * promote 的操作是替换锁类型，但锁对象不变。
     * S → X、IS → IX 是合法的 promotion。
     * promote(IS, SIX) 特殊，会释放子级的 S/IS 锁。
     * 要求 newLockType 是原有锁的“更强者”（substitutable），但不能相同。
     * 要维护 numChildLocks 状态。
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // 检查LockContext是否为只读
        if (readonly) {
            throw new UnsupportedOperationException("LockContext is readonly");
        }

        // 获取当前事务在该资源上持有的锁类型
        LockType lockType = getExplicitLockType(transaction);
        if (lockType == LockType.NL) {
            throw new NoLockHeldException("No lock held on " + name + " by " + transaction.getTransNum());
        }
        if (lockType == newLockType) {
            throw new DuplicateLockRequestException(transaction.getTransNum() + "transaction already has a newLockType lock on" + newLockType);
        }
        boolean validPromotion = LockType.substitutable(newLockType, lockType);
        boolean sixPromotion = newLockType == LockType.SIX &&
                (lockType == LockType.IS || lockType == LockType.IX || lockType == LockType.S);
        if (!validPromotion && !sixPromotion) {
            throw new InvalidLockException("Invalid lock promotion from " + lockType + " to " + newLockType);
        }

        // 检查是否有SIX祖先，如果有则不能升级到SIX、S或IS（会导致无效状态）
        if ((newLockType == LockType.SIX || newLockType == LockType.S || newLockType == LockType.IS) &&
                hasSIXAncestor(transaction)) {
            throw new InvalidLockException("Cannot promote to " + newLockType +
                    " when an ancestor holds a SIX lock");
        }

        // 处理从IS/IX/S升级到SIX的特殊情况
        List<ResourceName> releaseNames = new ArrayList<>();
        if (sixPromotion) {
            // 获取所有持有S或IS锁的后代资源
            List<ResourceName> sisResources = sisDescendants(transaction);

            // 释放所有后代上的S和IS锁
            for (ResourceName resourceName : sisResources) {
                LockContext descendantContext = fromResourceName(lockman, resourceName);

                // 更新父上下文的numChildLocks计数
                LockContext parentContext = descendantContext.parent;
                if (parentContext != null) {
                    parentContext.numChildLocks.compute(transaction.getTransNum(),
                            (k, v) -> v == null ? 0 : v - 1);
                }

                // 释放锁
                //lockman.release(transaction, resourceName);
                releaseNames.add(resourceName);
            }
            releaseNames.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);
        } else {
            // 执行锁升级
            //
//        releaseNames.add(name);
            //lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     *
     * 从多个子级别锁合并为一个当前级别的锁（向上聚合）。
     * 锁类型根据子锁类型自动选择（若含 X 就用 X，否则用 S）。
     * 不再持有任何子级别锁，简化锁结构。
     * 只做一次 lockManager.acquireAndRelease(...) 调用，保持原子性。
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        // 检查LockContext是否为只读
        if (readonly) {
            throw new UnsupportedOperationException("LockContext is readonly");
        }

        // 获取当前事务在该资源上持有的锁类型
        LockType currentLockType = getExplicitLockType(transaction);

        // 如果没有锁，抛出NoLockHeldException
        if (currentLockType == LockType.NL) {
            throw new NoLockHeldException("No lock held on " + name + " by " + transaction.getTransNum());
        }

        // 如果当前锁已经是X或S，并且没有子锁，则不需要做任何更改
        if ((currentLockType == LockType.X || currentLockType == LockType.S) &&
                getNumChildren(transaction) == 0) {
            return;
        }

        // 确定要升级到的锁类型
        LockType newLockType;
        if (currentLockType == LockType.IS || currentLockType == LockType.S) {
            newLockType = LockType.S;
        } else {
            // 对于IX, SIX, X，升级到X
            newLockType = LockType.X;
        }

        // 获取事务在当前上下文的所有后代节点上持有的锁
        List<ResourceName> descendantLocks = new ArrayList<>();
        // 使用LockManager的getLocks方法获取事务持有的所有锁
        List<Lock> allLocks = lockman.getLocks(transaction);

        // 筛选出属于当前上下文后代的锁
        for (Lock lock : allLocks) {
            ResourceName lockName = lock.name;
            // 检查是否是当前资源的后代
            if (lockName.isDescendantOf(name) && !lockName.equals(name)) {
                descendantLocks.add(lockName);
            }
        }

        // 如果没有后代锁且当前锁已经是目标锁类型，则不需要做任何更改
        if (descendantLocks.isEmpty() && currentLockType == newLockType) {
            return;
        }

        // 释放所有后代锁
        List<ResourceName> releaseNames = new ArrayList<>();
        for (ResourceName resourceName : descendantLocks) {
            LockContext descendantContext = fromResourceName(lockman, resourceName);

            // 更新父上下文的numChildLocks计数
            LockContext parentContext = descendantContext.parent;
            if (parentContext != null) {
                parentContext.numChildLocks.compute(transaction.getTransNum(),
                        (k, v) -> v == null ? 0 : v - 1);
            }

            // 释放锁
            //lockman.release(transaction, resourceName);
            releaseNames.add(resourceName);
        }

        // 如果当前锁不是目标锁类型，则需要升级或重新获取锁
        if (currentLockType != newLockType) {
            // 释放当前锁
//            lockman.release(transaction, name);
//            // 获取新锁
//            lockman.acquire(transaction, name, newLockType);
            //
            releaseNames.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);
        }

        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     *
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement

        // 首先检查当前资源上是否有显式锁
        LockType explicitLockType = getExplicitLockType(transaction);
        if (explicitLockType != LockType.NL) {
            return explicitLockType; // 如果有显式锁，直接返回
        }

        // 如果当前资源没有显式锁，检查祖先资源
        LockContext parent = parentContext();
        if (parent == null) {
            return LockType.NL; // 如果没有父上下文，返回NL
        }

        // 获取父上下文的有效锁类型
        LockType parentLockType = parent.getEffectiveLockType(transaction);

        // 根据父上下文的锁类型确定当前资源的有效锁类型
        if (parentLockType == LockType.S || parentLockType == LockType.X) {
            //根据多粒度锁的语义，如果一个事务可以读取或者读写一个资源，那么它也可以读取或者读写该资源的所有子资源
            return parentLockType; // S和X锁向下传递
        } else if (parentLockType == LockType.SIX) {
            // 共享意向排他锁有 S 锁的语意
            return LockType.S; // SIX锁向下传递为S锁
        } else {
            // 意向锁只是表示意向
            return LockType.NL; // IS、IX和NL锁不向下传递
        }
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    public boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // 从父上下文开始检查
        LockContext current = parent;

        // 向上遍历所有祖先上下文
        while (current != null) {
            // 如果祖先上下文持有SIX锁，返回true
            if (current.getExplicitLockType(transaction) == LockType.SIX) {
                return true;
            }
            current = current.parent;
        }

        // 没有找到持有SIX锁的祖先
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // 创建一个列表来存储结果
        List<ResourceName> result = new ArrayList<>();

        // 遍历所有子上下文
        for (LockContext childContext : children.values()) {
            // 获取子上下文的锁类型
            LockType childLockType = childContext.getExplicitLockType(transaction);

            // 如果子上下文持有S或IS锁，将其添加到结果中
            if (childLockType == LockType.S || childLockType == LockType.IS) {
                result.add(childContext.name);
            }

            // 递归检查子上下文的后代
            result.addAll(childContext.sisDescendants(transaction));
        }

        return result;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

