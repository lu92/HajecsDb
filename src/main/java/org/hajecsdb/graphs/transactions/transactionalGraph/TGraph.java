package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;

import java.util.Optional;
import java.util.Set;

public interface TGraph {
    Node createNode(Label label, Properties properties);
    Node setProperty(long nodeId, Property property);
    Optional<Node> getNodeById(long id);
    Set<Node> getAllNodes();
    Node deleteNode(long id);
    void commit();
    void rollback();
}
