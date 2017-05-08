package org.hajecsdb.graphs.transactions.lockMechanism;

import org.hajecsdb.graphs.core.Entity;

import java.util.HashMap;
import java.util.Map;

import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.READ;
import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.WRITE;

public class LockManager {
    public Map<LockUnit, Lock> lockedEntities = new HashMap<>();

    public void acquireWriteLock(Entity entity) {
        LockUnit lockUnit = new LockUnit(entity.getType(), entity.getId());
        if (lockedEntities.containsKey(lockUnit)) {
            Lock lock = lockedEntities.get(lockUnit);
            Lock upgradedLock = upgradeLock(lock);
            lockedEntities.put(lockUnit, upgradedLock);
        }
    }

    public void acquireReadLock(Entity entity) {
        LockUnit lockUnit = new LockUnit(entity.getType(), entity.getId());
        Lock lock = lockedEntities.containsKey(lockUnit) ? downgradeLock(lockedEntities.get(lockUnit)) : new RWLock(READ);
        lockedEntities.put(lockUnit, lock);
    }

    public Lock getReadLock(LockUnit lockUnit) throws LockNotFoundException {
        if (lockedEntities.containsKey(lockUnit)) {
            return lockedEntities.get(lockUnit);
        } else
            throw new LockNotFoundException();
    }


    private Lock upgradeLock(Lock lock) {
        if (lock.getType() == READ) {
            lock = new RWLock(WRITE);
        }
        return lock;
    }

    private Lock downgradeLock(Lock lock) {
        if (lock.getType() == WRITE) {
            lock = new RWLock(READ);
        }
        return lock;
    }
}
