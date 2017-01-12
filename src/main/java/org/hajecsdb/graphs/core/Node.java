package org.hajecsdb.graphs.core;

import java.util.Set;

public interface Node extends Entity {

    Set<Relationship> getRelationships();
    boolean hasRelationship();
    void addRelationShip(Relationship relationship);
    Iterable<Relationship> getRelationships( Direction direction, RelationshipType ... types );
    Iterable<Relationship> getRelationships(Direction direction);
    boolean hasRelationship( Direction dir );
    Iterable<RelationshipType> getRelationshipTypes();
    int getDegree();
    void setLabel(Label label );
    void removeLabel(Label label);
    Label getLabel();
    boolean hasLabel();
//    boolean hasLabel(Label label);
    void setProperties(Properties properties);

    Properties deleteProperties(String ... keys);
}
