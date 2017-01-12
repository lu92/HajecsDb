package org.hajecsdb.graphs.transactions;


public interface Transactional {
    void beginTransaction();
    Transaction getTransaction();
}
