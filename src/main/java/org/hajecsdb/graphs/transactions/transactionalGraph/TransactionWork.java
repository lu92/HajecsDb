package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.core.ResourceType;

import java.util.ArrayList;
import java.util.List;

import static org.hajecsdb.graphs.core.ResourceType.NODE;

public class TransactionWork {
    private long transactionId;
    private long entityId;
    private List<TransactionChange> transactionChanges = new ArrayList<>();
    private Node workingNode;
    private Relationship workingRelationship;
    private boolean deleted;

    public TransactionWork(long transactionId, Node node) {
        this.transactionId = transactionId;
        this.entityId = node.getId();
        this.workingNode = node;
        this.deleted = false;
    }

    public TransactionWork(long transactionId, Relationship relationship) {
        this.transactionId = transactionId;
        this.entityId = relationship.getId();
        this.workingRelationship = relationship;
    }

    public void addChange(TransactionChange transactionChange) {
        apply(transactionChange);
        this.transactionChanges.add(transactionChange);
    }

    public long getTransactionId() {
        return transactionId;
    }

    // concerns only operation on node's properties
    private void apply(TransactionChange transactionChange) {
        Entity entity = transactionChange.getResourceType() == NODE ? workingNode : workingRelationship;

        // in future Transaction Log will support this
        switch (transactionChange.getCrudType()) {
            case CREATE:
                entity.getAllProperties().add(transactionChange.getProperty());
                break;

            case READ:
                break;

            case UPDATE:
                entity.getAllProperties().delete(transactionChange.getProperty().getKey());
                entity.getAllProperties().add(transactionChange.getProperty());
                break;

            case DELETE:
                entity.getAllProperties().delete(transactionChange.getPropertyKey());
                break;
        }
    }


    public Entity readEntity(ResourceType resourceType) {
        switch (resourceType) {
            case NODE:
                return workingNode;

            case RELATIONSHIP:
                return workingRelationship;

            default:
                throw new IllegalArgumentException("Resource type is empty!");
        }
    }

    public Node readNode() {
        return workingNode;
    }

    public Relationship readRelationship() {
        return workingRelationship;
    }

    public void delete() {
        this.deleted = true;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TransactionWork that = (TransactionWork) o;

        if (transactionId != that.transactionId) return false;
        return entityId == that.entityId;
    }

    @Override
    public int hashCode() {
        int result = (int) (transactionId ^ (transactionId >>> 32));
        result = 31 * result + (int) (entityId ^ (entityId >>> 32));
        return result;
    }
}