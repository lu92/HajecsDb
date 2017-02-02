package org.hajecsdb.graphs.storage.serializers;


import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class NodeSerializer implements Serializer<Node, BinaryNode> {
    private Path nodesPath;
    private Path nodesMetaDataPath;
    private PropertiesBinaryMapper propertiesBinaryMapper;

    public NodeSerializer(Path nodesPath, Path nodesMetaDataPath) {
        this.nodesPath = nodesPath;
        this.nodesMetaDataPath = nodesMetaDataPath;
        this.propertiesBinaryMapper = new PropertiesBinaryMapper();
    }

    @Override
    public BinaryNode save(Node node) throws IOException {
        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(node.getAllProperties());
        BinaryNode binaryNode = new BinaryNode(node.getId(), binaryProperties);

        SeekableByteChannel nodesChannel = Files.newByteChannel(nodesPath, StandardOpenOption.APPEND);
        nodesChannel.write(ByteBuffer.wrap(binaryNode.getBytes()));
        nodesChannel.close();
        return binaryNode;
    }

    @Override
    public Node read(long id) throws IOException {
        return null;
    }

    @Override
    public Node update(Node node) throws IOException {
        return null;
    }

    @Override
    public void delete(long id) throws IOException {

    }
}
