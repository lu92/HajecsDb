package org.hajecsdb.graphs.storage.entities;

import java.nio.ByteBuffer;

public class PropertyHeader {
    private long beginBinaryPropertySection;
    private long endBinaryPropertySection;
    private BinaryProperty binaryProperty;

    public PropertyHeader(long beginIndex, long lastIndex, BinaryProperty binaryProperty) {
        this.beginBinaryPropertySection = beginIndex;
        this.endBinaryPropertySection = lastIndex;
        this.binaryProperty = binaryProperty;
        System.out.println("new header: " + "(" + beginBinaryPropertySection + ", " + endBinaryPropertySection + ")");
    }

    public int getLength() {
        return Long.BYTES + Long.BYTES + binaryProperty.getLength();
    }

    public byte[] getBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(getLength())
                .putLong(beginBinaryPropertySection)
                .putLong(endBinaryPropertySection)
                .put(binaryProperty.getBytes());
        return buffer.array();
    }
}