package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Property;

public class TransactionChange {
    private CRUDType crudType;
    private Property property;
    private String propertyKey;

    public TransactionChange(CRUDType crudType, Property property) {
        this.crudType = crudType;
        this.property = property;
    }

    public TransactionChange(CRUDType crudType, String propertyKey) {
        this.crudType = crudType;
        this.propertyKey = propertyKey;
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
}
