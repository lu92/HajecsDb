package org.hajecsdb.graphs.transactions.lockMechanism;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.transactions.Transaction;

import java.util.List;

public class LockingProtocolManager {

    // implementation of Strict 2-Phase-Locking Protocol

    private LockManager lockManager = new LockManager();
    private EntityLockRecognizer entityLockRecognizer = new EntityLockRecognizer();
    private CypherExecutor cypherExecutor = new CypherExecutor();

    public void provideLockedResources(Graph graph, Transaction transaction) {
        growingPhase(graph, transaction);
        transaction.getScope().getOperations()
                .forEach(operation -> cypherExecutor.execute(graph, operation.getCypherQuery()));
        transaction.commit();
        shrinkingPhase();
    }

    public void growingPhase(Graph graph, Transaction transaction) {
        transaction.getScope().getOperations().forEach(operation -> {
            List<Entity> entitiesRequiredToLock = entityLockRecognizer.determineEntities(graph, operation.getCypherQuery());
            for (Entity entity : entitiesRequiredToLock) {
//                if (operation.getLockType() == LockType.WRITE)
//                    lockManager.acquireWriteLock(entity);
//                else lockManager.acquireReadLock(entity);
            }
        });
    }

    public void shrinkingPhase() {

    }

}
