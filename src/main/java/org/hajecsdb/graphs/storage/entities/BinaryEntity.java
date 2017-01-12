package org.hajecsdb.graphs.storage.entities;

import java.nio.ByteBuffer;
import java.util.List;

public class BinaryEntity {
    private long start;
    private long end;
    private byte [] bytes;

    public BinaryEntity(long currentPosition, List<BinaryProperty> binaryProperties) {
        this.start = currentPosition;
        int totalBytes = binaryProperties.stream().mapToInt(binaryProperty -> binaryProperty.getBytes().length).sum();
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
        binaryProperties.stream().forEach(binaryProperty -> byteBuffer.put(binaryProperty.getBytes()));
        this.bytes = byteBuffer.array();
        this.end = currentPosition + bytes.length;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public int length() {
        return bytes.length;
    }

    public byte[] getBytes() {
        return bytes;
    }
}
