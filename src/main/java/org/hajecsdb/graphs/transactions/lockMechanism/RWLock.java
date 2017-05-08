package org.hajecsdb.graphs.transactions.lockMechanism;

import lombok.Data;

@Data
public class RWLock implements Lock {

    private boolean exclusive;
    private LockType lockType;

    public RWLock(LockType lockType) {
        this.exclusive = lockType == LockType.WRITE ? true : false;
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
    }
}
