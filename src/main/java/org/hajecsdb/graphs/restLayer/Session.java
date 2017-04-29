package org.hajecsdb.graphs.restLayer;


import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.transactions.*;

@Data
public class Session implements Transactional {

    private SessionPool sessionPool;
    private @Getter String sessionId;
    private Transaction transaction;
    private @Setter TransactionManager transactionManager;

    public Session(SessionPool sessionPool, String sessionId) {
        this.sessionPool = sessionPool;
        this.sessionId = sessionId;
    }

    void close() {
        sessionPool.closeSession(sessionId);
    }

    boolean isOpen() {
        return sessionPool.isSessionOpen(sessionId);
    }

    @Override
    public Transaction beginTransaction() {
        transaction = transactionManager.createTransaction();
        return transaction;
    }

    @Override
    public Transaction getTransaction() throws TransactionException {
        if (transaction == null)
            throw new TransactionException("The transaction has not been initiated!");
        return transaction;
    }


    public Result performQuery(String cypherQuery) {
        TransactionScope transactionScope = new TransactionScope();
//        transactionScope.
//        transaction.getScope().add();
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Session session = (Session) o;

        return sessionId.equals(session.sessionId);
    }

    @Override
    public int hashCode() {
        return sessionId.hashCode();
    }

    @Override
    public String toString() {
        return "Session{" +
                "sessionId='" + sessionId + '\'' +
                '}';
    }
}
