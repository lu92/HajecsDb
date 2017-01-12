package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Properties;

import java.util.Set;

public class GraphJsonTemplate {
    private Properties graphProperties;
    private Set<Properties> nodes;
    private Set<Properties> relationships;

    public Properties getGraphProperties() {
        return graphProperties;
    }

    public void setGraphProperties(Properties graphProperties) {
        this.graphProperties = graphProperties;
    }

    public Set<Properties> getNodes() {
        return nodes;
    }

    public void setNodes(Set<Properties> nodes) {
        this.nodes = nodes;
    }

    public Set<Properties> getRelationships() {
        return relationships;
    }

    public void setRelationships(Set<Properties> relationships) {
        this.relationships = relationships;
    }
}
