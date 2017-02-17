package org.hajecsdb.graphs.storage.entities;

import java.nio.ByteBuffer;

public class BinaryNode {
    private long nodeId;
    private BinaryProperties binaryProperties;
    private byte[] bytes;

    public BinaryNode(long nodeId, BinaryProperties binaryProperties) {
        this.nodeId = nodeId;
        this.binaryProperties = binaryProperties;
        this.bytes = ByteBuffer.allocate(Long.BYTES + binaryProperties.getLength())
                .putLong(nodeId).put(binaryProperties.getBytes()).array();
    }

    public byte[] getBytes() {
        return bytes;
    }
}
