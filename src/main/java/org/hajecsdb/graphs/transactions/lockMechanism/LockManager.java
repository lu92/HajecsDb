package org.hajecsdb.graphs.transactions.lockMechanism;

import org.hajecsdb.graphs.core.Entity;

import java.util.HashMap;
import java.util.Map;

import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.READ;
import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.WRITE;

public class LockManager {

    private Map<LockUnit, Lock> lockedEntities = new HashMap<>();

    public void acquireWriteLock(long transactionId, Entity entity) {
        LockUnit lockUnit = new LockUnit(entity.getType(), entity.getId());
        if (lockedEntities.containsKey(lockUnit)) {
            Lock lock = lockedEntities.get(lockUnit);
            Lock upgradedLock = upgradeLock(lock);
            lockedEntities.put(lockUnit, upgradedLock);
        } else {
            ReadWriteLock readWriteLock = new ReadWriteLock(transactionId, WRITE);
            readWriteLock.getW().lock();
            lockedEntities.put(lockUnit, readWriteLock);
        }
    }

    public void acquireReadLock(long transactionId, Entity entity) {
        LockUnit lockUnit = new LockUnit(entity.getType(), entity.getId());
        Lock Lock = lockedEntities.containsKey(lockUnit) ? downgradeLock(lockedEntities.get(lockUnit)) : new ReadWriteLock(transactionId, READ);
        lockedEntities.put(lockUnit, Lock);
    }

    public boolean isEntityLocked(Entity entity) {
        LockUnit searchedLockUnit = new LockUnit(entity.getType(), entity.getId());
        return lockedEntities.containsKey(searchedLockUnit);
    }


    private Lock upgradeLock(Lock lock) {
        if (lock.getType() == READ) {
            lock = new ReadWriteLock(lock.getTransactionId(), WRITE);
        } else {
            ((ReadWriteLock) lock).getW().lock();
        }
        return lock;
    }

    private Lock downgradeLock(Lock lock) {
        if (lock.getType() == WRITE) {
            lock = new ReadWriteLock(lock.getTransactionId(), READ);
        }
        return lock;
    }

    public void releaseLock(long transactionId, Entity entity) {
        LockUnit lockUnitReadyToRelease = new LockUnit(entity.getType(), entity.getId());
        if (lockedEntities.containsKey(lockUnitReadyToRelease)) {
            lockedEntities.get(lockUnitReadyToRelease).release();
            lockedEntities.remove(lockUnitReadyToRelease);
        }
        else
            throw new IllegalArgumentException(entity.getType() +  " [" + entity.getId() + "] is not locked!");
    }

    public Lock getLock(LockUnit lockUnit) throws LockNotFoundException {
        if (lockedEntities.containsKey(lockUnit))
            return lockedEntities.get(lockUnit);
        throw new LockNotFoundException();
    }
}
