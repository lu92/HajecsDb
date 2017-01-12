package org.hajecsdb.graphs.core;

import java.util.Optional;
import java.util.Set;

public interface Graph {
    String getPathDir();
    String getGraphName();
    String getFilename();
    Properties getProperties();
    Node createNode();
    Node createNode(Properties properties);
    Node createNode(Label label);
    Node createNode(Label label, Properties properties);
    Node addProperties(long nodeId, Properties properties);
    Optional<Node> getNodeById(long id);
    Set<Node> getAllNodes();
    Node deleteNode(long id);
    Relationship findRelationship(long beginNodeId, long endNodeId, RelationshipType relationshipType);
    Optional<Relationship> getRelationshipById(long id);
    Relationship deleteRelationship(long id);
    Set<Relationship> getAllRelationships();
    Iterable<Label> getAllLabels();
    Iterable<RelationshipType> getAllRelationshipTypes();
    Iterable<String> getAllPropertyKeys();
    Relationship createRelationship(Node beginNode, Node endNode, RelationshipType type );
    Relationship createRelationship(Node beginNode, String type, Node endNode);
}
