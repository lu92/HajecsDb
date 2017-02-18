package org.hajecsdb.graphs.storage.entities;

import java.nio.ByteBuffer;

public class NodeMetaData {

    public static final int SECTION_SIZE = Long.BYTES + 1 + 2*Long.BYTES;
    long nodeId;
    boolean deleted;            // 0 = deleted, otherwise not deleted
    long beginDataSection;      // begin index in nodes.bin
    long endDataSection;        // end index in nodes.bin

    public NodeMetaData(long nodeId, long beginDataSection, long endDataSection) {
        this.nodeId = nodeId;
        this.deleted = false;
        this.beginDataSection = beginDataSection;
        this.endDataSection = endDataSection;
    }

    public long getNodeId() {
        return nodeId;
    }

    public long getBeginDataSection() {
        return beginDataSection;
    }

    public long getEndDataSection() {
        return endDataSection;
    }

    public byte[] getBytes() {
        return ByteBuffer.allocate(SECTION_SIZE)
                .putLong(nodeId)
                .put((byte)(deleted == true ? 0 : 1))
                .putLong(beginDataSection)
                .putLong(endDataSection).array();
    }

    @Override
    public String toString() {
        return "NodeMetaData{" +
                "nodeId=" + nodeId +
                ", deleted=" + deleted +
                ", beginDataSection=" + beginDataSection +
                ", endDataSection=" + endDataSection +
                '}';
    }
}
