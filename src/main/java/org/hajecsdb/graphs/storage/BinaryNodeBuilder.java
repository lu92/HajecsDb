package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.storage.entities.BinaryNode;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;
import org.hajecsdb.graphs.storage.mappers.PropertiesBinaryMapper;

public class BinaryNodeBuilder {
    private BinaryNode binaryNode;
    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

    public BinaryNodeBuilder takeNode(Node node) {
        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(node.getAllProperties());
        binaryNode = new BinaryNode(node.getId(), binaryProperties);
        return this;
    }

    public BinaryNode build() {
        return binaryNode;
    }
}
