package org.hajecsdb.graphs.core;


public interface Relationship extends Entity{
    void setId(long id);
    Node getStartNode();
    void setStartNode(Node node);
    Node getEndNode();
    void setEndNode(Node node);
    Label getLabel();
    void setLabel(Label label);
    Direction getDirection();
    void setDirection(Direction direction);
    Relationship reverse();
    void setProperties(Properties properties);
    Properties deleteProperties(String ... keys);
    Relationship copy();
}
