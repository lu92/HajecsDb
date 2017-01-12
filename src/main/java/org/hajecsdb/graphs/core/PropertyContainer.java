package org.hajecsdb.graphs.core;

import java.util.Map;
import java.util.Optional;

public interface PropertyContainer {
    boolean hasProperty(String key);
    Optional<Property> getProperty(String key);
    void setProperty(String key, Object value);
    Property removeProperty(String key);
    Iterable<String> getPropertyKeys();
    Properties getProperties(String ... keys);
    Properties getAllProperties();
}
