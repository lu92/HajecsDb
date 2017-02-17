package org.hajecsdb.graphs.storage.entities;

import org.hajecsdb.graphs.storage.ByteUtils;

import java.nio.ByteBuffer;

public class NodeMetaData {

    public static final int SECTION_SIZE = 3*Long.BYTES;
    long nodeId;
    long beginDataSection;      // begin index in nodes.bin
    long endDataSection;        // end index in nodes.bin

//    private byte[] binaryNodeId;
//    private byte[] binaryBeginDataSection;
//    private byte[] binaryEndDataSection;

    public NodeMetaData(long nodeId, long beginDataSection, long endDataSection) {
        this.nodeId = nodeId;
        this.beginDataSection = beginDataSection;
        this.endDataSection = endDataSection;
//        this.binaryNodeId = ByteUtils.longToBytes(nodeId);
//        this.binaryBeginDataSection = ByteUtils.longToBytes(beginDataSection);
//        this.binaryEndDataSection = ByteUtils.longToBytes(endDataSection);
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
                .putLong(beginDataSection)
                .putLong(endDataSection).array();
    }

    @Override
    public String toString() {
        return "NodeMetaData{" +
                "nodeId=" + nodeId +
                ", beginDataSection=" + beginDataSection +
                ", endDataSection=" + endDataSection +
                '}';
    }
}
