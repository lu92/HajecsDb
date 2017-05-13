package org.hajecsdb.graphs.transactions.lockMechanism;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ReadWriteLock {
    private ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    Lock readLock = reentrantReadWriteLock.readLock();
    Lock writeLock = reentrantReadWriteLock.writeLock();

    private long transactionId;
    private LockType lockType;

    public ReadWriteLock(long transactionId, LockType lockType) {
        this.transactionId = transactionId;
        this.lockType = lockType;
    }
}
