package org.hajecsdb.graphs.transactions.lockMechanism;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;

import java.util.List;

public class LockingProtocolManager {

    // implementation of Strict 2-Phase-Locking Protocol

    private LockManager lockManager = new LockManager();
    private EntityLockRecognizer entityLockRecognizer = new EntityLockRecognizer();
    private CypherExecutor cypherExecutor = new CypherExecutor();

//    public void provideLockedResources(TransactionalGraphService graph, Transaction transaction) {
//        growingPhase(graph, transaction);
//        transaction.getScope().getOperations()
//                .forEach(operation -> cypherExecutor.execute(graph, transaction, operation.getCypherQuery()));
//        transaction.commit();
//        shrinkingPhase();
//    }

    public void growingPhase(TransactionalGraphService graph, Transaction transaction) {
        transaction.getScope().getOperations().forEach(operation -> {
            List<Entity> entitiesRequiredToLock = entityLockRecognizer.determineEntities(graph, transaction, operation.getCypherQuery());
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
