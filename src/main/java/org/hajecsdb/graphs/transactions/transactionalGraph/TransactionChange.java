package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Property;

public class TransactionChange {
    private long transactionId;
    private boolean commited;
    private CRUDType crudType;
    private Property property;
    private String propertyKey;

    public TransactionChange(long transactionId, boolean commited) {
        this.transactionId = transactionId;
        this.commited = commited;
    }

    public void setProperty(Property property) {
        this.crudType = CRUDType.CREATE;
        this.property = property;
    }

    public void updateProperty(Property property) {
        this.crudType = CRUDType.UPDATE;
        this.property = property;
    }

    public void deleteProperty(String key) {
        this.crudType = CRUDType.DELETE;
        this.propertyKey = key;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public CRUDType getCrudType() {
        return crudType;
    }

    public Property getProperty() {
        return property;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public boolean isCommited() {
        return commited;
    }
}
