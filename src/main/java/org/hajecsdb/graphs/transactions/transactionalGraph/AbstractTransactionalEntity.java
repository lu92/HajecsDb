package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.NotFoundException;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.ResourceType;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hajecsdb.graphs.transactions.transactionalGraph.CRUDType.*;

abstract class AbstractTransactionalEntity {
    private ResourceType resourceType;
    protected boolean committed;
    protected List<TransactionWork> transactionWorkList = new ArrayList<>();


    protected AbstractTransactionalEntity(ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    abstract void createTransactionWork(long transactionId);

    public synchronized void delete(long transactionId) {
        if (!isTransactionWorkExists(transactionId)) {
            createTransactionWork(transactionId);
        }
        TransactionWork transactionWork = getTransactionWork(transactionId);
        transactionWork.delete();
    }

    public synchronized void addTransactionChange(long transactionId, TransactionChange change) {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        transactionWork.addChange(change);
    }

    protected boolean isTransactionWorkExists(long transactionId) {
        return transactionWorkList.stream()
                .anyMatch(transactionWork -> transactionWork.getTransactionId() == transactionId);
    }

    protected TransactionWork getTransactionWork(long transactionId) {
        Optional<TransactionWork> transactionWorkOptional = transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.getTransactionId() == transactionId)
                .findFirst();

        if (!transactionWorkOptional.isPresent())
            throw new TransactionException("");

        return transactionWorkOptional.get();
    }

    public boolean containsTransactionChanges(long transactionId) {
        return this.transactionWorkList.stream()
                .anyMatch(transactionWork -> transactionWork.getTransactionId() == transactionId);
    }

    public boolean isDeleted(long transactionId) {
        Optional<TransactionWork> transactionWorkOptional = transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.getTransactionId() == transactionId)
                .findFirst();
        return transactionWorkOptional.isPresent() && transactionWorkOptional.get().isDeleted();
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setProperty(long transactionId, Property property) {
        if (!isTransactionWorkExists(transactionId)) {
            createTransactionWork(transactionId);
        }
        Entity workingEntity = getWorkingEntity(transactionId);
        if (!workingEntity.hasProperty(property.getKey())) {
            TransactionChange change = new TransactionChange(resourceType, CREATE, property);
            addTransactionChange(transactionId, change);
        } else {
            TransactionChange change = new TransactionChange(resourceType, UPDATE, property);
            addTransactionChange(transactionId, change);
        }
    }

    public void deleteProperty(long transactionId, String propertyKey) {
        if (!isTransactionWorkExists(transactionId)) {
            createTransactionWork(transactionId);
        }
        Entity workingNode = getWorkingEntity(transactionId);
        if (workingNode.hasProperty(propertyKey)) {
            TransactionChange change = new TransactionChange(resourceType, DELETE, propertyKey);
            addTransactionChange(transactionId, change);
        } else
            throw new NotFoundException("Property '" + propertyKey + "' was not found!");
    }

    public Entity getWorkingEntity(long transactionId) {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        if (transactionWork.isDeleted())
            return null;
        return transactionWork.readEntity(resourceType);
    }
}
