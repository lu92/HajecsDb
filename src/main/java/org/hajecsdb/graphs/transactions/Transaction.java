package org.hajecsdb.graphs.transactions;

public interface Transaction {
    long getId();
    TransactionStatus commit();
    TransactionStatus rollback();
    TransactionStatus getStatus();
    TransactionScope getScope();
    boolean isPerformed();
}
