package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Relationship;

import java.util.Set;

public interface GraphToJsonMapper {
    GraphJsonTemplate mapToGraphJsonTemplate(Graph graph);
    Properties mapNode(Node node);
    Set<Properties> mapNodes(Set<Node> node);
    Properties mapRelationship(Relationship relationship);
    Set<Properties> mapRelationships(Set<Relationship> relationships);
}
