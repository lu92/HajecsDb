package org.hajecsdb.graphs.transactions.lockMechanism;

import lombok.Data;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
public class RWMLock implements MLock {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private boolean exclusive;
    private long transactionId;
    private LockType lockType;

    public RWMLock(long transactionId, LockType lockType) {
        this.exclusive = lockType == LockType.WRITE ? true : false;
        this.transactionId = transactionId;
        this.lockType = lockType;
    }

    @Override
    public LockType getType() {
        return lockType;
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    @Override
    public void release() {
        w.unlock();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final RWMLock rwmLock = (RWMLock) o;

        if (transactionId != rwmLock.transactionId) return false;
        return lockType == rwmLock.lockType;
    }

    @Override
    public int hashCode() {
        int result = (int) (transactionId ^ (transactionId >>> 32));
        result = 31 * result + lockType.hashCode();
        return result;
    }
}
