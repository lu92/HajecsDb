package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;

import java.util.ArrayList;
import java.util.List;

public class TransactionWork {
    private long transactionId;
    private List<TransactionChange> transactionChanges = new ArrayList<>();
    private Node workingNode;
    private Relationship workingRelationship;

    public TransactionWork(long transactionId, Node node) {
        this.transactionId = transactionId;
        this.workingNode = node;
    }

    public TransactionWork(long transactionId, Relationship relationship) {
        this.transactionId = transactionId;
        this.workingRelationship = relationship;
    }

    public void addChange(TransactionChange transactionChange) {
        apply(transactionChange);
        this.transactionChanges.add(transactionChange);
    }

    public long getTransactionId() {
        return transactionId;
    }

    public Node getWorkingNode() {
        return workingNode;
    }

    public Relationship getWorkingRelationship() {
        return workingRelationship;
    }

    // concerns only operation on node's properties
    private void apply(TransactionChange transactionChange) {
        // in future Transaction Log will support this
        switch (transactionChange.getCrudType()) {
            case CREATE_NODES_PROPERTY:
                workingNode.getAllProperties().add(transactionChange.getProperty());
                break;

            case READ_NODES_PROPERTY:
                break;

            case UPDATE_NODES_PROPERTY:
                workingNode.getAllProperties().delete(transactionChange.getProperty().getKey());
                workingNode.getAllProperties().add(transactionChange.getProperty());
                break;

            case DELETE_NODES_PROPERTY:
                workingNode.getAllProperties().delete(transactionChange.getPropertyKey());
                break;

            case CREATE_RELATIONSHIPS_PROPERTY:
                workingRelationship.getAllProperties().add(transactionChange.getProperty());
                break;
        }
    }


    public Node readNode() {
        return workingNode;
    }

    public Relationship readRelationship() {
        return workingRelationship;
    }
}