package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import static org.hajecsdb.graphs.core.ResourceType.NODE;

public class TNode extends AbstractTransactionalEntity {
    private Node originNode;

    public TNode(Node node) {
        super(NODE);
        this.originNode = node;
    }

    @Override
    public void createTransactionWork(long transactionId) {
        Node nodeCopy = originNode.copy();
        TransactionWork transactionWork = new TransactionWork(transactionId, nodeCopy);
        this.transactionWorkList.add(transactionWork);
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
        this.originNode = (Node) transactionWork.readEntity(NODE);
        this.transactionWorkList.remove(transactionWork);
        committed = true;
    }

    public Node getOriginNode() {
        return originNode;
    }
}
