package org.hajecsdb.graphs.transactions;

public interface Transactional {
    Transaction beginTransaction();
    Transaction getTransaction();
}
