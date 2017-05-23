package org.hajecsdb.graphs.transactions.lockMechanism;

import lombok.Data;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
public class ReadWriteLock implements Lock {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final java.util.concurrent.locks.Lock r = rwl.readLock();
    private final java.util.concurrent.locks.Lock w = rwl.writeLock();

    private boolean exclusive;
    private long transactionId;
    private LockType lockType;

    public ReadWriteLock(long transactionId, LockType lockType) {
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
        return transactionId;
    }

    @Override
    public void release() {
        w.unlock();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ReadWriteLock readWriteLock = (ReadWriteLock) o;

        if (transactionId != readWriteLock.transactionId) return false;
        return lockType == readWriteLock.lockType;
    }

    @Override
    public int hashCode() {
        int result = (int) (transactionId ^ (transactionId >>> 32));
        result = 31 * result + lockType.hashCode();
        return result;
    }
}
