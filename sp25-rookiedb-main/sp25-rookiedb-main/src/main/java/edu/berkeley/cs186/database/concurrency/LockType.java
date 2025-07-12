package edu.berkeley.cs186.database.concurrency;

import java.util.Map;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    private static int getIndex(LockType lockType) {
        switch (lockType) {
            case NL -> {
                return 0;
            }
            case IS -> {
                return 1;
            }
            case IX -> {
                return 2;
            }
            case S -> {
                return 3;
            }
            case SIX -> {
                return 4;
            }
            case X -> {
                return 5;
            }
            default -> {
                throw new UnsupportedOperationException("bad lock type");
            }
        }
    }

    private static final boolean[][] compatibility = {
            //          NL    IS    IX     S    SIX     X
            /* NL */ { true, true, true, true, true, true },
            /* IS */ { true, true, true, true, true, false },
            /* IX */ { true, true, true, false, false, false },
            /* S  */ { true, true, false, true, false, false },
            /* SIX*/ { true, true, false, false, false, false },
            /* X  */ { true, false, false, false, false, false }
    };

    private static final boolean[][] substitutionTable = {
            //         NL     IS     IX     S      SIX    X
            /* NL  */ { true, false, false, false, false, false },
            /* IS  */ { true, true,  false, false, false, false },
            /* IX  */ { true, true,  true,  false, false, false },
            /* S   */ { true, false, false, true,  false, false },
            /* SIX */ { true, true,  true,  true,  true,  false },
            /* X   */ { true, true,  true,  true,  true,  true }
    };

    /**
     * parentLockType\childLockType
     * 子资源加 NL（不加锁），父资源任何锁都可以
     * 子资源加 IS，父资源可以是IS,IX,SIX,X
     * 子资源加 X（排他锁），父资源至少X,IX,SIX
     * 子资源加 S（共享锁），父资源至少要持有S,IS,IX,SIX
     * 子资源加 SIX（意向共享排他），父资源至少要持有X,IX,SIX
     *
     */
    private static final boolean[][] canBeParent = {
            //          NL     IS     IX     S      SIX    X
            /* NL  */ { true,  false, false, false, false, false },
            /* IS  */ { true,  true,  false,  true,  false, false },
            /* IX  */ { true,  true,  true,  true, true,  true  },
            /* S   */ { true,  false, false, true, false, false },
            /* SIX */ { true,  true,  true,  true, true,  true  },
            /* X   */ { true,  true,  true,  true,  true,  true  }
    };

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return compatibility[getIndex(a)][getIndex(b)];

    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     * 这个规则是多粒度锁中的 意向锁机制，确保不会发生冲突地在资源树中并发加锁
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return canBeParent[getIndex(parentLockType)][getIndex(childLockType)];
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return substitutionTable[getIndex(substitute)][getIndex(required)];
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

