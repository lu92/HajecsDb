package org.hajecsdb.graphs.storage.entities;

import java.nio.ByteBuffer;

public class MetaData {

    public static final int SECTION_SIZE = Long.BYTES + 1 + 2*Long.BYTES;
    private long id;
    private boolean deleted;            // 0 = deleted, otherwise not deleted
    private long beginDataSection;      // begin index in entity bin file
    private long endDataSection;        // end index in entity bin file

    public MetaData(long id, long beginDataSection, long endDataSection) {
        this.id = id;
        this.deleted = false;
        this.beginDataSection = beginDataSection;
        this.endDataSection = endDataSection;
    }

    public long getId() {
        return id;
    }
    public long getBeginDataSection() {
        return beginDataSection;
    }

    public long getEndDataSection() {
        return endDataSection;
    }

    public byte[] getBytes() {
        return ByteBuffer.allocate(SECTION_SIZE)
                .putLong(id)
                .put((byte)(deleted ? 0 : 1))
                .putLong(beginDataSection)
                .putLong(endDataSection).array();
    }

    @Override
    public String toString() {
        return "MetaData{" +
                "id=" + id +
                ", deleted=" + deleted +
                ", beginDataSection=" + beginDataSection +
                ", endDataSection=" + endDataSection +
                '}';
    }
}