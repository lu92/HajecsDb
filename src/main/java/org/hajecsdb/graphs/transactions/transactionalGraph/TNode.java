package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TNode {
    private Node originNode;
    private boolean commited;
    private boolean deleted;
    private List<TransactionWork> transactionWorkList = new ArrayList<>();

    public TNode(long transactionId, Node node) {
        this.originNode = node;
        this.commited = false;
        this.deleted = false;
        createTransactionWork(transactionId);
    }

    public Node getWorkingNode(long transacionId) {
        return getTransactionWork(transacionId).workingNode;
    }

    private void createTransactionWork(long transactionId) {
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
                .filter(transactionWork -> transactionWork.transactionId == transactionId)
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
                .anyMatch(transactionWork -> transactionWork.transactionId == transactionId);
    }

    public Node setProperty(long transactionId, Property property) {
        if (!isTransactionWorkExists(transactionId)) {
            createTransactionWork(transactionId);
        }

        Node workingNode = getWorkingNode(transactionId);
        if (!workingNode.hasProperty(property.getKey())) {
            TransactionChange change = new TransactionChange(transactionId, false);
            change.setProperty(property);
            addTransactionChange(transactionId, change);
        } else {
            TransactionChange change = new TransactionChange(transactionId, false);
            change.updateProperty(property);
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
        commited = true;
    }

    public Node getOriginNode() {
        return originNode;
    }

    public boolean isCommited() {
        return commited;
    }

    public boolean containsTransactionChanges(long transactionId) {
        return this.transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.transactionId == transactionId)
                .findAny().isPresent();
    }

    class TransactionWork {
        private long transactionId;
        private List<TransactionChange> transactionChanges = new ArrayList<>();
        private Node workingNode;

        public TransactionWork(long transactionId, Node node) {
            this.transactionId = transactionId;
            this.workingNode = node;
        }

        public void addChange(TransactionChange transactionChange) {
            apply(transactionChange);
            this.transactionChanges.add(transactionChange);
        }

        // concerns only operation on node's properties
        private void apply(TransactionChange transactionChange) {
            // in future Transaction Log will support this
            switch (transactionChange.getCrudType()) {
                case CREATE:
                    workingNode.getAllProperties().add(transactionChange.getProperty());
                    break;

                case READ:
                    break;

                case UPDATE:
                    workingNode.getAllProperties().delete(transactionChange.getProperty().getKey());
                    workingNode.getAllProperties().add(transactionChange.getProperty());
                    break;

                case DELETE:
                    workingNode.getAllProperties().delete(transactionChange.getPropertyKey());
                    break;
            }
        }


        public Node readNode() {
            return workingNode;
        }
    }
}
