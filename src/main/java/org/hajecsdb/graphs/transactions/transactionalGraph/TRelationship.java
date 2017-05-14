package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Direction;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.core.impl.RelationshipImpl;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import static org.hajecsdb.graphs.core.ResourceType.NODE;
import static org.hajecsdb.graphs.core.ResourceType.RELATIONSHIP;

class TRelationship extends AbstractTransactionalEntity {
    private Relationship originRelationship;
    private TNode startTNode;
    private TNode endTNode;

    public TRelationship(long transactionId, long relationshipId, TNode startTNode, TNode endTNode, Label label) {
        super(RELATIONSHIP);
        this.startTNode = startTNode;
        this.endTNode = endTNode;

        this.originRelationship = new RelationshipImpl(
                relationshipId,
                ((Node) startTNode.getWorkingEntity(transactionId)),
                ((Node) endTNode.getWorkingEntity(transactionId)),
                Direction.OUTGOING, label);
    }

    @Override
    void createTransactionWork(long transactionId) {
        Relationship relationshipCopy = originRelationship.copy();
        TransactionWork transactionWork = new TransactionWork(transactionId, relationshipCopy);
//        if (!startTNode.containsTransactionChanges(transactionId)) {
//            startTNode.createTransactionWork(transactionId);
//        }
        ((Node) startTNode.getWorkingEntity(transactionId)).addRelationShip(relationshipCopy);
//        TransactionChange appendedRelationshipChange = new TransactionChange(NODE, CRUDType.APPEND_RELATIONSHIP, relationshipCopy);
//        startTNode.addTransactionChange(transactionId, appendedRelationshipChange);
//        endTNode.addTransactionChange(transactionId, appendedRelationshipChange);

//        if (!endTNode.containsTransactionChanges(transactionId)) {
//            endTNode.createTransactionWork(transactionId);
//        }
        ((Node) endTNode.getWorkingEntity(transactionId)).addRelationShip(relationshipCopy);
        this.transactionWorkList.add(transactionWork);
    }

    public synchronized void commitTransaction(long transactionId) throws TransactionException {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        this.originRelationship = transactionWork.readRelationship();
        this.transactionWorkList.remove(transactionWork);
        committed = true;
    }

    public synchronized void rollbackTransaction(long transactionId) throws TransactionException {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        this.transactionWorkList.remove(transactionWork);
    }

    public Relationship getOriginRelationship() {
        return originRelationship;
    }
}
