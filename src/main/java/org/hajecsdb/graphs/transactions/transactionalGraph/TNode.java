package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.NotFoundException;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hajecsdb.graphs.transactions.transactionalGraph.CRUDType.*;

public class TNode {
    private Node originNode;
    private boolean committed;
    private boolean deleted;
    private List<TransactionWork> transactionWorkList = new ArrayList<>();

    public TNode(long transactionId, Node node) {
        this.originNode = node;
        this.committed = false;
        this.deleted = false;
        createOrGetTransactionWork(transactionId);
    }

    private TransactionWork createOrGetTransactionWork(long transactionId) {
        if (!isTransactionWorkExists(transactionId))
            createTransactionWork(transactionId);
        return getTransactionWork(transactionId);
    }

    public Node getWorkingNode(long transactionId) {
        createOrGetTransactionWork(transactionId);

        TransactionWork transactionWork = getTransactionWork(transactionId);
        if (transactionWork.isDeleted())
            return null;
        return transactionWork.getWorkingNode();

    }

    public void createTransactionWork(long transactionId) {
        Node nodeCopy = originNode.copy();
        TransactionWork transactionWork = new TransactionWork(transactionId, nodeCopy);
        this.transactionWorkList.add(transactionWork);
    }

    public synchronized void addTransactionChange(long transactionId, TransactionChange change) {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        transactionWork.addChange(change);
    }

    private TransactionWork getTransactionWork(long transactionId) {
        Optional<TransactionWork> transactionWorkOptional = transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.getTransactionId() == transactionId)
                .findFirst();

        if (!transactionWorkOptional.isPresent())
            throw new TransactionException("");

        return transactionWorkOptional.get();
    }

    public boolean isDeleted() {
        return deleted;
    }

    public synchronized Node readNode(long transactionId) {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        return transactionWork.readNode();
    }

    private boolean isTransactionWorkExists(long transactionId) {
        return transactionWorkList.stream()
                .anyMatch(transactionWork -> transactionWork.getTransactionId() == transactionId);
    }

    public Node setProperty(long transactionId, Property property) {
        createOrGetTransactionWork(transactionId);


        Node workingNode = getWorkingNode(transactionId);
        if (!workingNode.hasProperty(property.getKey())) {
            TransactionChange change = new TransactionChange(CREATE_NODES_PROPERTY, property);
            addTransactionChange(transactionId, change);
        } else {
            TransactionChange change = new TransactionChange(UPDATE_NODES_PROPERTY, property);
            addTransactionChange(transactionId, change);
        }

        return null;
    }

    public synchronized Node deleteNode(long transactionId) {
        createOrGetTransactionWork(transactionId);
        TransactionWork transactionWork = getTransactionWork(transactionId);
        transactionWork.deleteNode();
        Node node = transactionWork.readNode();
        return node;
    }

    public synchronized void rollbackTransaction(long transactionId) throws TransactionException {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        this.transactionWorkList.remove(transactionWork);
    }

    public synchronized void commitTransaction(long transactionId) throws TransactionException {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        this.originNode = transactionWork.readNode();
        this.transactionWorkList.remove(transactionWork);
        committed = true;
    }

    public Node getOriginNode() {
        return originNode;
    }

    public boolean isCommitted() {
        return committed;
    }

    public boolean containsTransactionChanges(long transactionId) {
        return this.transactionWorkList.stream()
                .anyMatch(transactionWork -> transactionWork.getTransactionId() == transactionId);
    }

    public void deleteProperty(long transactionId, String propertyKey) {
        createOrGetTransactionWork(transactionId);

        Node workingNode = getWorkingNode(transactionId);
        if (workingNode.hasProperty(propertyKey)) {
            TransactionChange change = new TransactionChange(DELETE_NODES_PROPERTY, propertyKey);
            addTransactionChange(transactionId, change);
        } else
            throw new NotFoundException("Property '" + propertyKey + "' was not found!");

    }

    public boolean isDeleted(long transactionId) {
        Optional<TransactionWork> transactionWorkOptional = transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.getTransactionId() == transactionId)
                .findFirst();
        if (!transactionWorkOptional.isPresent())
            return false;

        return transactionWorkOptional.get().isDeleted();
    }
}
