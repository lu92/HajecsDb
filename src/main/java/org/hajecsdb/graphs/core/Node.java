package org.hajecsdb.graphs.core;

import java.util.Set;

public interface Node extends Entity {
    Set<Relationship> getRelationships();
    boolean hasRelationship();
    void addRelationShip(Relationship relationship);
    Iterable<Relationship> getRelationships( Direction direction, Label ... labels );
    Iterable<Relationship> getRelationships(Direction direction);
    boolean hasRelationship( Direction dir );
    int getDegree();
    void setLabel(Label label );
    void removeLabel(Label label);
    Label getLabel();
    boolean hasLabel();
    void setProperties(Properties properties);
    Properties deleteProperties(String ... keys);
    Node copy();
}
