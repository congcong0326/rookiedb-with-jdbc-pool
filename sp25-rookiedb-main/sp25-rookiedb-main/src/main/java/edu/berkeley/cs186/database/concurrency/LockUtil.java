package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */

    private static final Logger logger = LoggerFactory.getLogger(LockUtil.class);


    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        //assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) {
            logger.debug("ensureSufficientLockHeld: transaction or lockContext is null, returning early");
            return;
        }

        logger.debug("ensureSufficientLockHeld: transaction={}, lockContext={}, requestType={}",
                transaction.getTransNum(), lockContext.name, requestType);

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        // 事物在该资源隐式持有的锁，包含继承关系
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        // 事物在该资源显示持有的锁
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        logger.debug("Current lock state: effectiveLockType={}, explicitLockType={}, parentContext={}",
                effectiveLockType, explicitLockType,
                parentContext != null ? parentContext.name : "null");

        // 如果请求的锁类型是NL，不需要做任何事情
        if (requestType == LockType.NL) {
            logger.debug("Request type is NL, no action needed");
            return;
        }

        // 如果当前有效锁类型已经可以替代请求的锁类型，不需要做任何事情
        if (LockType.substitutable(effectiveLockType, requestType)) {
            logger.debug("Current effective lock type {} can substitute requested type {}, no action needed",
                    effectiveLockType, requestType);
            return;
        }

        // 处理当前锁类型是IX，请求的锁类型是S的特殊情况
        // 这种情况下，我们需要升级到SIX锁
        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            logger.debug("Special case: IX -> S, checking for SIX ancestor");
            // 确保祖先节点上没有SIX锁（否则会导致无效状态）
            if (!lockContext.hasSIXAncestor(transaction)) {
                logger.debug("No SIX ancestor found, promoting IX to SIX");
                lockContext.promote(transaction, LockType.SIX);
                logger.debug("Successfully promoted to SIX lock");
                return;
            } else {
                logger.debug("SIX ancestor exists, cannot promote to SIX");
            }
        }
        if (explicitLockType == LockType.S && requestType == LockType.IX) {
            // S锁不能直接升级到IX，应该升级到SIX
            logger.debug("Cannot promote S to IX directly, promoting to SIX instead");
            lockContext.promote(transaction, LockType.SIX);
            logger.debug("Successfully promoted S to SIX lock");
            return;
        }

        // 处理当前锁类型是意向锁的情况
        if (explicitLockType.isIntent()) {
            logger.debug("Current lock is intent lock: {}", explicitLockType);
            // 如果当前锁是意向锁，需要升级到请求的锁类型
            // 对于IS升级到S，IX/SIX升级到X
            if ((explicitLockType == LockType.IS && requestType == LockType.S) ||
                    ((explicitLockType == LockType.IX || explicitLockType == LockType.SIX) && requestType == LockType.X)) {
                logger.debug("Escalating intent lock {} to {}", explicitLockType, requestType);
                lockContext.escalate(transaction);
                logger.debug("Successfully escalated lock");
                return;
            }
        }

        // 其他情况：需要确保祖先节点上有适当的锁，然后获取请求的锁
        logger.debug("General case: ensuring parent locks for request type {}", requestType);
        ensureParentLocks(transaction, parentContext, requestType);

        // 如果当前没有锁，直接获取
        if (explicitLockType == LockType.NL) {
            logger.debug("No current lock, acquiring {} lock", requestType);
            lockContext.acquire(transaction, requestType);
            logger.debug("Successfully acquired {} lock", requestType);
        } else if (LockType.substitutable(explicitLockType, requestType)) {
            // 如果请求的锁类型可以替代当前锁，不需要做任何事情
            logger.debug("Current explicit lock {} can substitute requested type {}, no action needed",
                    explicitLockType, requestType);
            return;
        } else {
            // 否则，需要升级锁
            logger.debug("Promoting lockContext {} lock from {} to {}", lockContext, explicitLockType, requestType);
            lockContext.promote(transaction, requestType);
            logger.debug("Successfully promoted lock to {}", requestType);
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    private static void ensureParentLocks(TransactionContext transaction, LockContext parentContext, LockType requestType) {
        // 如果没有父节点，不需要做任何事情
        if (parentContext == null) {
            return;
        }

        // 计算父节点需要的锁类型
        LockType parentLockType = LockType.parentLock(requestType);

        // 递归确保祖先节点上有适当的锁
        ensureSufficientLockHeld(parentContext, parentLockType);
    }
}
