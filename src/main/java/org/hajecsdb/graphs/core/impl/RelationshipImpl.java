package org.hajecsdb.graphs.core.impl;

import org.hajecsdb.graphs.core.*;
import java.util.Optional;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

public class RelationshipImpl implements Relationship {
    private Node startNode;
    private Node endNode;
    private Label label;
    private Direction direction;
    private Properties properties;

    public RelationshipImpl(long id) {
        properties = new Properties();
        this.properties.add(new Property(ID, LONG, id));
    }


    public RelationshipImpl(long id, Node startNode, Node endNode, Direction direction, Label label) {
        this.startNode = startNode;
        this.endNode = endNode;
        this.direction = direction;
        this.label = label;
        properties = new Properties();
        properties.add(ID, id, LONG);
        properties.add(new Property("startNode", LONG, startNode.getId()));
        properties.add(new Property("endNode", LONG, endNode.getId()));
        properties.add(new Property("label", STRING, label.getName()));
        properties.add(new Property("direction", STRING, direction.toString()));
    }

    @Override
    public void setId(long id) {
        this.properties.delete(ID);
        this.properties.add(new Property(ID, LONG, id));
    }

    @Override
    public Node getStartNode() {
        return startNode;
    }

    @Override
    public void setStartNode(Node node) {
        this.startNode = node;
    }

    @Override
    public Node getEndNode() {
        return endNode;
    }

    @Override
    public void setEndNode(Node node) {
        this.endNode = node;
    }

    @Override
    public Label getLabel() {
        return label;
    }

    @Override
    public void setLabel(Label label) {
        this.label = label;
        this.properties.delete(LABEL);
        this.properties.add(new Property(LABEL, STRING, label.getName()));
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    @Override
    public Relationship reverse() {
        return new RelationshipImpl(getId(), endNode, startNode, getDirection().reverse(), label);
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties.addAll(properties);
    }

    @Override
    public Properties deleteProperties(String... keys) {
        Properties deletedProperties = new Properties();
        for (String key : keys) {
            Property deletedProperty = properties.delete(key);
            deletedProperties.add(deletedProperty);
        }
        return deletedProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final RelationshipImpl that = (RelationshipImpl) o;

        if (startNode.getId() != that.startNode.getId()) return false;
        if (endNode.getId() != that.endNode.getId()) return false;
        if (!label.getName().equals(that.label.getName())) return false;
        return direction == that.direction;
    }

    @Override
    public int hashCode() {
        int result = startNode.hashCode();
        result = 31 * result + endNode.hashCode();
        result = 31 * result + label.hashCode();
        result = 31 * result + direction.hashCode();
        return result;
    }

    @Override
    public String toString() {
        if (direction == Direction.OUTGOING) {
            return "Relationship (" + startNode.getId() + ")-[" + label.getName() + "]->(" + endNode.getId() + ")";
        } else {
            return "Relationship (" + startNode.getId() + ")<-[" + label.getName() + "]-(" + endNode.getId() + ")";
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
        return properties.getProperty(key);
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
