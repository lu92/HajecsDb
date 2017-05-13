package org.hajecsdb.graphs.transactions.transactionalGraph;

public enum CRUDType {
    CREATE, READ, UPDATE, DELETE,

    CREATE_NODE, DELETE_NODE,
    CREATE_RELATIONSHIP, DELETE_RELATIONSHIP,

    APPEND_RELATIONSHIP, REMOVE_RELATIONSHIP
}
