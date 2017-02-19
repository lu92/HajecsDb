package org.hajecsdb.graphs.storage.entities;

import java.nio.ByteBuffer;

public class RelationshipMetaData {
    public static final int SECTION_SIZE = Long.BYTES + 1 + 2*Long.BYTES;
    private long relationshipId;
    boolean deleted;            // 0 = deleted, otherwise not deleted
    long beginDataSection;      // begin index in relationship.bin
    long endDataSection;        // end index in relationship.bin

    public RelationshipMetaData(long relationshipId, long beginDataSection, long endDataSection) {
        this.relationshipId = relationshipId;
        this.deleted = false;
        this.beginDataSection = beginDataSection;
        this.endDataSection = endDataSection;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public long getBeginDataSection() {
        return beginDataSection;
    }

    public long getEndDataSection() {
        return endDataSection;
    }

    public byte[] getBytes() {
        return ByteBuffer.allocate(SECTION_SIZE)
                .putLong(relationshipId)
                .put((byte)(deleted == true ? 0 : 1))
                .putLong(beginDataSection)
                .putLong(endDataSection).array();
    }

    @Override
    public String toString() {
        return "RelationshipMetaData{" +
                "relationshipId=" + relationshipId +
                ", deleted=" + deleted +
                ", beginDataSection=" + beginDataSection +
                ", endDataSection=" + endDataSection +
                '}';
    }
}
