package org.hajecsdb.graphs.impl;

import org.hajecsdb.graphs.core.*;

import java.util.Map;
import java.util.Optional;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

public class RelationshipImpl implements Relationship {
    private Node startNode;
    private Node endNode;
    private RelationshipType relationshipType;
    private Direction direction;
    private Properties properties;


    public RelationshipImpl(long id, Node startNode, Node endNode, Direction direction, RelationshipType relationshipType) {
        this.startNode = startNode;
        this.endNode = endNode;
        this.direction = direction;
        this.relationshipType = relationshipType;
        properties = new Properties();
        properties.add(ID, id, LONG);
        properties.add(new Property("startNode", startNode.getId(), LONG));
        properties.add(new Property("endNode", endNode.getId(), LONG));
        properties.add(new Property("RelationshipType", relationshipType.getName(), STRING));
        properties.add(new Property("direction", direction, STRING));
    }

    @Override
    public Node getStartNode() {
        return startNode;
    }

    @Override
    public Node getEndNode() {
        return endNode;
    }

    @Override
    public RelationshipType getType() {
        return relationshipType;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public Relationship reverse() {
        return new RelationshipImpl(getId(), endNode, startNode, getDirection().reverse(), relationshipType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final RelationshipImpl that = (RelationshipImpl) o;

        if (startNode.getId() != that.startNode.getId()) return false;
        if (endNode.getId() != that.endNode.getId()) return false;
        if (!relationshipType.getName().equals(that.relationshipType.getName())) return false;
        return direction == that.direction;
    }

    @Override
    public int hashCode() {
        int result = startNode.hashCode();
        result = 31 * result + endNode.hashCode();
        result = 31 * result + relationshipType.hashCode();
        result = 31 * result + direction.hashCode();
        return result;
    }

    @Override
    public String toString() {
        if (direction == Direction.OUTGOING) {
            return "Relationship (" + startNode.getId() + ")-[" + relationshipType.getName() + "]->(" + endNode.getId() + ")";
        } else {
            return "Relationship (" + startNode.getId() + ")<-[" + relationshipType.getName() + "]-(" + endNode.getId() + ")";
        }
    }

    @Override
    public long getId() {
        return (long) properties.getProperty(ID).get().getValue();
    }

    @Override
    public boolean hasProperty(String key) {
        return false;
    }

    @Override
    public Optional<Property> getProperty(String key) {
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {

    }

    @Override
    public Property removeProperty(String key) {
        return null;
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return null;
    }

    @Override
    public Properties getProperties(String... keys) {
        return null;
    }

    @Override
    public Properties getAllProperties() {
        return properties;
    }
}
