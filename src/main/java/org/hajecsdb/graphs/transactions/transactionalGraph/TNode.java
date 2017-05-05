package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hajecsdb.graphs.transactions.transactionalGraph.CRUDType.CREATE_NODES_PROPERTY;
import static org.hajecsdb.graphs.transactions.transactionalGraph.CRUDType.UPDATE_NODES_PROPERTY;

public class TNode {
    private Node originNode;
    private boolean committed;
    private boolean deleted;
    private List<TransactionWork> transactionWorkList = new ArrayList<>();

    public TNode(long transactionId, Node node) {
        this.originNode = node;
        this.committed = false;
        this.deleted = false;
        createTransactionWork(transactionId);
    }

    public Node getWorkingNode(long transactionId) {
        return getTransactionWork(transactionId).getWorkingNode();
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
        if (!isTransactionWorkExists(transactionId)) {
            createTransactionWork(transactionId);
        }

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
        if (!isTransactionWorkExists(transactionId)) {
            createTransactionWork(transactionId);
        }
        TransactionWork transactionWork = getTransactionWork(transactionId);
        this.deleted = true;
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
}
