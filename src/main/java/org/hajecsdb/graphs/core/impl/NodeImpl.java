package org.hajecsdb.graphs.core.impl;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.core.Properties;

import java.util.*;
import java.util.stream.Collectors;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;


public class NodeImpl implements Node {

    private Set<Relationship> relationships = new HashSet<>();

    private Properties properties = new Properties();

    public NodeImpl(long id) {
        this.properties.add(new Property(ID, LONG, id));
    }

    @Override
    public long getId() {
        return (long) properties.getProperty(ID).get().getValue();
    }

    @Override
    public ResourceType getType() {
        return ResourceType.NODE;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public boolean hasRelationship() {
        return false;
    }

    @Override
    public void addRelationShip(Relationship relationship) {
        this.relationships.add(relationship);
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction, Label ... labels) {
        List<String> typeList = Arrays.asList(labels).stream().map(Label::getName).collect(Collectors.toList());
        return relationships.stream()
                .filter(relationship -> relationship.getDirection() == direction
                        && typeList.contains(relationship.getLabel().getName())).collect(Collectors.toSet());
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction dir) {
        return null;
    }

    @Override
    public boolean hasRelationship(Direction dir) {
        return false;
    }

    @Override
    public int getDegree() {
        return relationships.size();
    }

    @Override
    public void setLabel(Label label) {
        properties.add(new Property(LABEL, STRING, label.getName()));
    }

    @Override
    public void removeLabel(Label label) {

    }

    @Override
    public Label getLabel() {
        return properties.hasProperty(LABEL) ? new Label((String) properties.getProperty(LABEL).get().getValue()) : null;
    }

    @Override
    public boolean hasLabel() {
        return getLabel() != null;
    }

//    @Override
//    public boolean hasLabel(Label label) {
//        return false;
//    }

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
    public Node copy() {
        NodeImpl copy = new NodeImpl(getId());
        copy.setLabel(getLabel());
        Properties propertiesCopy = new Properties();
        List<Property> properties = getAllProperties().getAllProperties().stream().map(Property::copy).collect(Collectors.toList());
        propertiesCopy.addAll(properties);
        copy.setProperties(propertiesCopy);
        copy.getRelationships().addAll(this.getRelationships().stream().map(relationship -> relationship.copy()).collect(Collectors.toSet()));
        return copy;
    }

    @Override
    public boolean hasProperty(String key) {
        return getAllProperties().hasProperty(key);
    }

    @Override
    public Optional<Property> getProperty(String key) {
        return properties.getProperty(key);
    }


    @Override
    public void setProperty(String key, Object value) {
        Optional<Property> property = properties.getProperty(key);
        if (property.isPresent()) {
            PropertyType type = property.get().getType();
            properties.delete(key);
            properties.add(key, value, type);
        }
    }

    @Override
    public Property removeProperty(String key) {
        return properties.delete(key);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final NodeImpl node = (NodeImpl) o;

        return properties != null ? properties.equals(node.properties) : node.properties == null;
    }

    @Override
    public int hashCode() {
        return (int) getId();
    }
}
