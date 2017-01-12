package org.hajecsdb.graphs.storage.entities;

import org.hajecsdb.graphs.storage.ByteUtils;

public class NodeMetaData {
    long nodeId;
    long beginDataSection;
    long endDataSection;
    long nextNodeMetaData;
    long prevNodeMetaData;

    private byte[] binaryNodeId;
    private byte[] binaryBeginDataSection;
    private byte[] binaryEndDataSection;
    private byte[] binaryNextNodeMetaData;
    private byte[] binaryPrevNodeMetaData;

    public NodeMetaData(long nodeId, long beginDataSection, long endDataSection, long nextNodeMetaData, long prevNodeMetaData) {
        this.nodeId = nodeId;
        this.beginDataSection = beginDataSection;
        this.endDataSection = endDataSection;
        this.nextNodeMetaData = nextNodeMetaData;
        this.prevNodeMetaData = prevNodeMetaData;
        this.binaryNodeId = ByteUtils.longToBytes(nodeId);
        this.binaryBeginDataSection = ByteUtils.longToBytes(beginDataSection);
        this.binaryEndDataSection = ByteUtils.longToBytes(endDataSection);
        this.binaryNextNodeMetaData = ByteUtils.longToBytes(nextNodeMetaData);
        this.binaryPrevNodeMetaData = ByteUtils.longToBytes(prevNodeMetaData);
    }
}
