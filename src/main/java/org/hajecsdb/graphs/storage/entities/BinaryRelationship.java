package org.hajecsdb.graphs.storage.entities;


import java.nio.ByteBuffer;

public class BinaryRelationship {
    private long relationshipId;
    private BinaryProperties binaryProperties;
    private byte [] bytes;

    public BinaryRelationship(long relationshipId, BinaryProperties binaryProperties) {
        this.relationshipId = relationshipId;
        this.binaryProperties = binaryProperties;
        this.bytes = ByteBuffer.allocate(Long.BYTES + binaryProperties.getLength())
                .putLong(relationshipId).put(binaryProperties.getBytes()).array();
    }

    public byte[] getBytes() {
        return bytes;
    }
}
