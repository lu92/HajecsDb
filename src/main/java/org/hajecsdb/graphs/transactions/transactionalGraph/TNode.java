package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TNode {
    private Node originNode;
    private List<TransactionWork> transactionWorkList = new ArrayList<>();

    public TNode(Node node) {
        this.originNode = node;
    }

    public List<TransactionChange> getTransactionChanges(long transactionId) {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        return transactionWork.transactionChanges;
    }

    public synchronized void addTransactionChange(long transactionId, TransactionChange change) {
        TransactionWork transactionWork = createOrGetTransactionWork(transactionId);
        transactionWork.addChange(change);
    }

    private TransactionWork createOrGetTransactionWork(long transactionId) {
        Optional<TransactionWork> transactionWorkOptional = transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.transactionId == transactionId)
                .findFirst();

        if (!transactionWorkOptional.isPresent()) {
            Node nodeCopy = originNode.copy();
            TransactionWork transactionWork = new TransactionWork(transactionId, nodeCopy);
            this.transactionWorkList.add(transactionWork);
        }

        return getTransactionWork(transactionId);
    }

    private TransactionWork getTransactionWork(long transactionId) {
        Optional<TransactionWork> transactionWorkOptional = transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.transactionId == transactionId)
                .findFirst();

        if (!transactionWorkOptional.isPresent())
            throw new TransactionException("");

        return transactionWorkOptional.get();
    }

    public synchronized Node readNode(long transactionId) {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        return transactionWork.readNode();
    }

    public synchronized void rollbackTransaction(long transactionId) throws TransactionException {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        this.transactionWorkList.remove(transactionWork);
    }

    public synchronized void commitTransaction(long transactionId) throws TransactionException {
        TransactionWork transactionWork = getTransactionWork(transactionId);
        this.originNode = transactionWork.readNode();
        this.transactionWorkList.remove(transactionWork);

    }

    public Node getOriginNode() {
        return originNode;
    }

    public boolean containsTransactionChanges(long transactionId) {
        return this.transactionWorkList.stream()
                .filter(transactionWork -> transactionWork.transactionId == transactionId)
                .findAny().isPresent();
    }

    class TransactionWork {
        private long transactionId;
        private boolean commited;
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

        private void apply(TransactionChange transactionChange) {
            // in future transactionlog will support this
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
