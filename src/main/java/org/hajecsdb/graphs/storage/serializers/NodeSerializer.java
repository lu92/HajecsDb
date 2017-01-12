package org.hajecsdb.graphs.storage.serializers;


import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.storage.EntityType;

import java.io.IOException;
import java.nio.file.Path;

public class NodeSerializer {
    private Path nodesPath;
    private Path nodesMetaDataPath;
    private EntitySerializer entitySerializer = new EntitySerializer();

    public void createNode(Node node) throws IOException {
        entitySerializer.serializeEntity(nodesPath, nodesMetaDataPath, node, EntityType.NODE);
    }
}
