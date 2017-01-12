package org.hajecsdb.graphs.transactions;

import org.hajecsdb.graphs.storage.GraphStorage;

public class TransactionManager {
    private int defaultTimeout;
    private int maximumTimeout;


    public Transaction createTransaction() {
        return new Transaction() {
            private TransactionStatus transactionStatus = new TransactionStatus();
            private TransactionScope transactionScope = new TransactionScope();

            @Override
            public boolean commit() {
                return false;
            }

            @Override
            public boolean rollback() {
                return false;
            }

            @Override
            public TransactionStatus getStatus() {
                return transactionStatus;
            }

            @Override
            public TransactionScope getScope() {
                return transactionScope;
            }
        };
    }
}
