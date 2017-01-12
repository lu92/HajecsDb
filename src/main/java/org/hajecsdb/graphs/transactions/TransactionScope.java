package org.hajecsdb.graphs.transactions;

import org.hajecsdb.graphs.core.Entity;

import java.util.LinkedList;
import java.util.Queue;

public class TransactionScope {
    private Queue<OperationElement> operationsQueue = new LinkedList();

    public TransactionScope() {
    }

    public void add(Entity entity, OperationType type) {
        operationsQueue.add(new OperationElement(entity, type));
    }

    public Queue<OperationElement> getOperationsQueue() {
        return operationsQueue;
    }
}
