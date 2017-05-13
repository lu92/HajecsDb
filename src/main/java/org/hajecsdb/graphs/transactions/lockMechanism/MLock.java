package org.hajecsdb.graphs.transactions.lockMechanism;

public interface MLock {

    LockType getType();
    long getTransactionId();

    /**
     * Releases this lock before the transaction finishes. It is an optional
     * operation and if not called, this lock will be released when the owning
     * transaction finishes.
     *
     * @throws IllegalStateException if this lock has already been released.
     */
    void release();
}
