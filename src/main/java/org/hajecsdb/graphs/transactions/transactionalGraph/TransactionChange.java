package org.hajecsdb.graphs.transactions.transactionalGraph;

import lombok.Getter;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.ResourceType;

public class TransactionChange {
    private @Getter ResourceType resourceType;
    private @Getter CRUDType crudType;
    private @Getter Property property;
    private @Getter String propertyKey;

    public TransactionChange(ResourceType type, CRUDType crudType, Property property) {
        this.resourceType = type;
        this.crudType = crudType;
        this.property = property;
    }

    public TransactionChange(ResourceType type, CRUDType crudType, String propertyKey) {
        this.resourceType = type;
        this.crudType = crudType;
        this.propertyKey = propertyKey;
    }
}
