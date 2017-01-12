package org.hajecsdb.graphs.transactions;

public interface Transaction {
    boolean commit();
    boolean rollback();
    TransactionStatus getStatus();
    TransactionScope getScope();
}
