package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Direction;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.core.impl.RelationshipImpl;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hajecsdb.graphs.transactions.transactionalGraph.CRUDType.CREATE_RELATIONSHIPS_PROPERTY;

class TRelationship {
    Relationship originRelationship;
    private TNode startTNode;
    private TNode endTNode;
    private Label label;
    private boolean committed;
    private boolean deleted;
    private List<TransactionWork> transactionWorkList = new ArrayList<>();

    public TRelationship(long transactionId, long relationshipId, TNode startTNode, TNode endTNode, Label label) {
        this.startTNode = startTNode;
        this.endTNode = endTNode;
        this.label = label;
        this.committed = false;
        this.deleted = false;
        Relationship relationship = new RelationshipImpl(
                relationshipId,
                startTNode.getWorkingNode(transactionId), endTNode.getWorkingNode(transactionId), Direction.OUTGOING, label);

        this.originRelationship = relationship;
        createTransactionWork(transactionId);
    }

    private void createTransactionWork(long transactionId) {
        Relationship relationshipCopy = originRelationship.copy();
        TransactionWork transactionWork = new TransactionWork(transactionId, relationshipCopy);
        if (!startTNode.containsTransactionChanges(transactionId)) {
            startTNode.createTransactionWork(transactionId);
        }
        startTNode.getWorkingNode(transactionId).addRelationShip(relationshipCopy);

        if (!endTNode.containsTransactionChanges(transactionId)) {
            endTNode.createTransactionWork(transactionId);
        }
        endTNode.getWorkingNode(transactionId).addRelationShip(relationshipCopy);
        this.transactionWorkList.add(transactionWork);
    }

    public void setProperty(long transactionId, Property property) {
        if (!isTransactionWorkExists(transactionId)) {
            createTransactionWork(transactionId);
        }

        Relationship workingRelationship = getWorkingRelationship(transactionId);
        if (!workingRelationship.hasProperty(property.getKey())) {
            TransactionChange change = new TransactionChange(CREATE_RELATIONSHIPS_PROPERTY, property);
            addTransactionChange(transactionId, change);
        }
    }

    public synchronized void addTransactionChange(long transactionId, TransactionChange change) {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        transactionWork.addChange(change);
    }

    private boolean isTransactionWorkExists(long transactionId) {
        return transactionWorkList.stream()
                .anyMatch(transactionWork -> transactionWork.getTransactionId() == transactionId);
    }

    public synchronized void commitTransaction(long transactionId) throws TransactionException {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        this.originRelationship = transactionWork.readRelationship();
        this.transactionWorkList.remove(transactionWork);
        committed = true;
    }

    private TransactionWork getTransactionWork(long transactionId) {
        Optional<TransactionWork> transactionWorkOptional = transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.getTransactionId() == transactionId)
                .findFirst();

        if (!transactionWorkOptional.isPresent())
            throw new TransactionException("");

        return transactionWorkOptional.get();
    }

    public boolean containsTransactionChanges(long transactionId) {
        return this.transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.getTransactionId() == transactionId)
                .findAny().isPresent();
    }

    public boolean isCommitted() {
        return committed;
    }

    public Relationship getOriginRelationship() {
        return originRelationship;
    }

    public Relationship getWorkingRelationship(long transactionId) {
        return getTransactionWork(transactionId).getWorkingRelationship();
    }


}
