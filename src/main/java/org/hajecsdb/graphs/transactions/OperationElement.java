package org.hajecsdb.graphs.transactions;

import org.hajecsdb.graphs.core.Entity;

public class OperationElement {
    private Entity entity;
    private OperationType operationType;

    public OperationElement(Entity entity, OperationType operationType) {
        this.entity = entity;
        this.operationType = operationType;
    }

    public Entity getEntity() {
        return entity;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final OperationElement that = (OperationElement) o;

        if (!entity.equals(that.entity)) return false;
        return operationType == that.operationType;
    }

    @Override
    public int hashCode() {
        int result = entity.hashCode();
        result = 31 * result + operationType.hashCode();
        return result;
    }
}
