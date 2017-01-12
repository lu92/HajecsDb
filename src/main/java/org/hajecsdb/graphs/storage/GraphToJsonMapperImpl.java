package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Relationship;

import java.util.Set;
import java.util.stream.Collectors;

public class GraphToJsonMapperImpl implements GraphToJsonMapper{

    @Override
    public GraphJsonTemplate mapToGraphJsonTemplate(Graph graph) {
        GraphJsonTemplate graphJsonTemplate = new GraphJsonTemplate();
        graphJsonTemplate.setGraphProperties(graph.getProperties());
        graphJsonTemplate.setNodes(mapNodes(graph.getAllNodes()));
        graphJsonTemplate.setRelationships(mapRelationships(graph.getAllRelationships()));
        return graphJsonTemplate;
    }

    @Override
    public Properties mapNode(Node node) {
        return node.getAllProperties();
    }

    @Override
    public Set<Properties> mapNodes(Set<Node> nodes) {
        return nodes.stream().map(this::mapNode).collect(Collectors.toSet());
    }

    @Override
    public Properties mapRelationship(Relationship relationship) {
        return relationship.getAllProperties();
    }

    @Override
    public Set<Properties> mapRelationships(Set<Relationship> relationships) {
        return relationships.stream().map(this::mapRelationship).collect(Collectors.toSet());
    }


}
