package org.hajecsdb.graphs.storage.entities;

import org.hajecsdb.graphs.storage.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BinaryNode {
    private transient long nodeId;

    private byte[] binaryNodeId;
    private byte[] binaryPropertyCount;
    private byte[] bytes;

    class BinaryProperties { // niech bedzie w nodes.bin
        private int propertyCount;
        private List<PropertyMetaData> propertyMetaDataList;
        private long startPropertySection;
        private long endPropertySection;
        private byte[] payload; // binary properties
    }

    public BinaryNode(long nodeId, long currentPosition, List<BinaryProperty> binaryProperties) {
        mapNodeId(nodeId);
        mapPropertyCount(binaryProperties.size());

//        this.propertyMetaDataList = new ArrayList<>(binaryProperties.size());

        int totalBytes = binaryProperties.stream().mapToInt(binaryProperty -> binaryProperty.getBytes().length).sum();
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
        binaryProperties.stream().forEach(binaryProperty -> byteBuffer.put(binaryProperty.getBytes()));
//        this.bytes = byteBuffer.array();
    }

    private void mapPropertyCount(int propertyCount) {
//        this.propertyCount = propertyCount;
        this.binaryPropertyCount = ByteUtils.intToBytes(propertyCount);
    }

    private void mapNodeId(long nodeId) {
        this.nodeId = nodeId;
        this.binaryNodeId = ByteUtils.longToBytes(nodeId);
    }
}
