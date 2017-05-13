package org.hajecsdb.graphs.transactions.lockMechanism;

import org.hajecsdb.graphs.core.Entity;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.READ;
import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.WRITE;

public class LockManager {


//    class LockedEntities {
//        private Map<LockUnit, Set<MLock>> lockedEntities = new HashMap<>();
//
//        void addLock(LockUnit lockUnit, MLock mLock) {
//            if (lockedEntities.containsKey(lockUnit))
//                lockedEntities.get(lockUnit).add(mLock);
//            else {
//
//            }
//        }
//
//    }

    public Map<LockUnit, MLock> lockedEntities = new HashMap<>();

    public void acquireWriteLock(long transactionId, Entity entity) {
        LockUnit lockUnit = new LockUnit(entity.getType(), entity.getId());
        if (lockedEntities.containsKey(lockUnit)) {
            MLock mLock = lockedEntities.get(lockUnit);
            MLock upgradedMLock = upgradeLock(mLock);
            lockedEntities.put(lockUnit, upgradedMLock);
        } else {
            RWMLock rwmLock = new RWMLock(transactionId, WRITE);
            rwmLock.getW().lock();
            lockedEntities.put(lockUnit, rwmLock);
        }
    }

    public void acquireReadLock(long transactionId, Entity entity) {
        LockUnit lockUnit = new LockUnit(entity.getType(), entity.getId());
        MLock MLock = lockedEntities.containsKey(lockUnit) ? downgradeLock(lockedEntities.get(lockUnit)) : new RWMLock(transactionId, READ);
        lockedEntities.put(lockUnit, MLock);
    }

    public MLock getReadLock(LockUnit lockUnit) throws LockNotFoundException {
        if (lockedEntities.containsKey(lockUnit)) {
            return lockedEntities.get(lockUnit);
        } else
            throw new LockNotFoundException();
    }

    public boolean isEntityLocked(Entity entity) {
        LockUnit searchedLockUnit = new LockUnit(entity.getType(), entity.getId());
        return lockedEntities.containsKey(searchedLockUnit);
    }


    private MLock upgradeLock(MLock mLock) {
        if (mLock.getType() == READ) {
            mLock = new RWMLock(mLock.getTransactionId(), WRITE);
        } else {
            ((RWMLock) mLock).getW().lock();
        }
        return mLock;
    }

    private MLock downgradeLock(MLock mLock) {
        if (mLock.getType() == WRITE) {
            mLock = new RWMLock(mLock.getTransactionId(), READ);
        }
        return mLock;
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

    public MLock getLock(LockUnit lockUnit) {
        return lockedEntities.get(lockUnit);
    }
}
