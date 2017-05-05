package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.*;

import java.util.Optional;
import java.util.Set;

public interface TGraph {
    Node createNode(Label label, Properties properties);
    Node setPropertyToNode(long nodeId, Property property);
    Optional<Node> getNodeById(long id);
    Set<Node> getAllNodes();
    Node deleteNode(long id);
    Relationship createRelationship(long startNodeId, long endNodeId, Label label);
    Relationship setPropertyToRelationship(long relationshipId, Property property);
    Relationship deleteRelationship(long id);
    void commit();
    void rollback();
}
