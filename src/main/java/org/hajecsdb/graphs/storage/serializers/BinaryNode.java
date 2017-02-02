package org.hajecsdb.graphs.storage.serializers;

import org.hajecsdb.graphs.storage.entities.BinaryProperties;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BinaryNode {
    private byte[] bytes;

    public BinaryNode(long nodeId, BinaryProperties binaryProperties) {
        bytes = ByteBuffer.allocate(Long.BYTES + binaryProperties.getLength())
                .putLong(nodeId)
                .put(binaryProperties.getBytes())
                .array();
    }

    public byte[] getNodeIdInBinaryFigure() {
        return Arrays.copyOfRange(bytes, 0, Long.BYTES);
    }

    public byte[] getPropertiesInBinaryFigure() {
        return Arrays.copyOfRange(bytes, Long.BYTES, bytes.length);
    }

    public long getNodeId() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(getNodeIdInBinaryFigure());
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public byte[] getBytes() {
        return bytes;
    }
}
