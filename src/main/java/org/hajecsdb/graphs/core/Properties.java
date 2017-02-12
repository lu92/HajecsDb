package org.hajecsdb.graphs.core;


import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Properties {
    private Set<Property> properties;

    public Properties() {
        this.properties = new HashSet<>();
    }

    public Properties add(String key, Object value, PropertyType type) {
        return add(new Property(key, type, value));
    }

    public Properties add(Property property) {
        if (property == null || StringUtils.isEmpty(property.getKey()) || property.getValue() == null || property.getType() == null)
            throw new IllegalArgumentException("key, value and type must be initiated!");

        this.properties.add(property);
        return this;
    }

    public Property delete(String key) {
        Optional<Property> property = getProperty(key);
        if (!property.isPresent()) {
            throw new NotFoundException("Property does not exist!");
        }
        this.properties.remove(property.get());
        return property.get();
    }

    public int size() {
        return this.properties.size();
    }

    public boolean hasProperty(String key) {
        return properties.stream().anyMatch(property -> property.getKey().equals(key));
    }

    public Optional<Property> getProperty(String key) {
        return properties.stream().filter(property -> property.getKey().equals(key)).findFirst();
    }

    public Set<String> getKeys() {
        return properties.stream().map(Property::getKey).collect(Collectors.toSet());
    }

    public Set<Property> getAllProperties() {
        return properties;
    }

    public void addAll(Properties properties) {
        for (Property property : properties.getAllProperties()) {
            this.properties.add(property);
        }
    }

    public Properties getProperties(Set<String> keys) {
        boolean containsSelectedProperties = keys.stream().allMatch(this::hasProperty);
        if (!containsSelectedProperties) {
            throw new NotFoundException("Some property not founded!");
        }

        Properties properties = new Properties();
        for (String key : keys) {
            properties.add(getProperty(key).get());
        }
        return properties;
    }
}