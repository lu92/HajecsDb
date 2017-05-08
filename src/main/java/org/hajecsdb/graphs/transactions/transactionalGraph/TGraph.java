package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.*;

import java.util.Optional;
import java.util.Set;

public interface TGraph {
    Node createNode(Label label, Properties properties);
    Optional<Node> getNodeById(long nodeId);
    Set<Node> getAllNodes();
    Node deleteNode(long nodeId);
    Node setPropertyToNode(long nodeId, Property property);
    void deletePropertyFromNode(long nodeId, String propertyKey);
    Relationship createRelationship(long startNodeId, long endNodeId, Label label);
    Relationship deleteRelationship(long relationshipId);
    Relationship setPropertyToRelationship(long relationshipId, Property property);
    void deletePropertyFromRelationship(int relationshipId, String propertyKey);
    void commit();
    void rollback();
}
