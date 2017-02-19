package org.hajecsdb.graphs.core;


public interface Relationship extends Entity{
    Node getStartNode();
    Node getEndNode();
    RelationshipType getType();
    Direction getDirection();
    Relationship reverse();
    void setProperties(Properties properties);
    Properties deleteProperties(String ... keys);
}
