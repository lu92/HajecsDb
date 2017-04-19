package org.hajecsdb.graphs.storage.serializers;


import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.impl.NodeImpl;

public class NodeSerializer extends EntitySerializer<Node> {

    public NodeSerializer(String nodesFilename, String nodeMetadataFilename) {
        super(nodesFilename, nodeMetadataFilename);
    }

    @Override
    Node buildEntityType(long id, Properties properties) {
        Node node = new NodeImpl(id);
        node.setProperties(properties);
        return node;
    }
}
