package org.hajecsdb.graphs.transactions.transactionalGraph;

import lombok.Getter;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.core.ResourceType;

public class TransactionChange {
    private @Getter ResourceType resourceType;
    private @Getter CRUDType crudType;
    private @Getter Relationship relationship;
    private @Getter Property property;
    private @Getter String propertyKey;

    public TransactionChange(ResourceType type, CRUDType crudType) {
        this.resourceType = type;
        this.crudType = crudType;
    }

    public TransactionChange(ResourceType type, CRUDType crudType, Relationship relationship) {
        this.resourceType = type;
        this.crudType = crudType;
        this.relationship = relationship;
    }

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
