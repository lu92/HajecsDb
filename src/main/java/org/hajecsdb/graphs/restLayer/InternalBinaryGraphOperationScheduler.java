package org.hajecsdb.graphs.restLayer;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.storage.BinaryGraphStorage;
import org.hajecsdb.graphs.storage.GraphStorage;
import org.hajecsdb.graphs.transactions.OperationType;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class InternalBinaryGraphOperationScheduler {

    private BlockingQueue<CRUDOperation> crudOperationQueue = new LinkedBlockingQueue<>();
    private GraphStorage graphStorage = new BinaryGraphStorage();
    private Thread consumerThread;

    public InternalBinaryGraphOperationScheduler() {
        consumerThread = new Thread(new Consumer(crudOperationQueue));
        consumerThread.start();
    }

    public void add(String command, Result result) {
        CRUDOperation crudOperation = new CRUDOperation(command, getCommandType(command), result);
        try {
            crudOperationQueue.put(crudOperation);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class Consumer implements Runnable {
        private BlockingQueue<CRUDOperation> crudOperationQueue;

        public Consumer(BlockingQueue<CRUDOperation> crudOperationQueue) {
            this.crudOperationQueue = crudOperationQueue;
        }

        @Override
        public void run() {
            while(true) {
                try {
                    CRUDOperation crudOperation = crudOperationQueue.take();
                    System.out.println("[CRUD Operation] performing: " + crudOperation.getCommand());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    private OperationType getCommandType(String command) {
        if (command.contains("DELETE")) {
            return OperationType.DELETE;
        } else if (command.contains("SET")) {
            return OperationType.UPDATE;
        } else if (command.contains("CREATE")) {
            return OperationType.CREATE;
        } else return OperationType.READ;
    }

    @Data
    @AllArgsConstructor
    class CRUDOperation {
        private String command;
        private OperationType type;
        private Result result;
    }
}
