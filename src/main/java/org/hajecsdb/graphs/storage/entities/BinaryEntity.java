package org.hajecsdb.graphs.storage.entities;

import java.nio.ByteBuffer;

public class BinaryEntity {
    private long entityId;
    private BinaryProperties binaryProperties;
    private byte [] bytes;

    public BinaryEntity(long entityd, BinaryProperties binaryProperties) {
        this.entityId = entityd;
        this.binaryProperties = binaryProperties;
        this.bytes = ByteBuffer.allocate(Long.BYTES + binaryProperties.getLength())
                .putLong(entityd).put(binaryProperties.getBytes()).array();
    }

    public byte[] getBytes() {
        return bytes;
    }
}
