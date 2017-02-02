package org.hajecsdb.graphs.storage.entities;

import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.storage.serializers.PropertiesBinaryMapper;
import org.hajecsdb.graphs.storage.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BinaryProperties {
    int numberOfProperties;
    long beginIndex;
    transient long lastIndex; // is set on last index
    List<PropertyHeader> propertyHeaderList = new ArrayList<>();
    byte[] bytes;

    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

    public BinaryProperties() {
        numberOfProperties = 0;
        beginIndex = 0;
        lastIndex = Integer.BYTES;
        bytes = ByteBuffer.allocate(Integer.BYTES).putInt(numberOfProperties).array();
    }

    public void addProperty(BinaryProperty binaryProperty) {
        PropertyHeader propertyHeader = new PropertyHeader(lastIndex, binaryProperty);
        numberOfProperties++;
        propertyHeaderList.add(propertyHeader);
        lastIndex += propertyHeader.getLength();
        invalidateBytes(propertyHeaderList);
    }

    private void invalidateBytes(List<PropertyHeader> propertyHeaderList) {
        int totalBytes = Integer.BYTES + propertyHeaderList.stream()
                .mapToInt(binaryProperty -> binaryProperty.getLength())
                .sum();

        ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes).putInt(numberOfProperties);

        propertyHeaderList.stream().forEach(header -> byteBuffer.put(header.getBytes()));
        bytes = byteBuffer.array();
    }


//    // pamietaj o przesunieciu wszystkiego przy zapisie
//    public void shiftIndexes(long shift) {
//        for (PropertyHeader propertyHeader : propertyHeaderList) {
//            propertyHeader.beginBinaryPropertySection += shift;
//            propertyHeader.endBinaryPropertySection += shift;
//        }
//        invalidateBytes(propertyHeaderList);
//    }

    public List<BinaryProperty> getBinaryProperties() {
        List<BinaryProperty> binaryProperties = new ArrayList<>(numberOfProperties);
        if (numberOfProperties == 0)
            return binaryProperties;

        long begin = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, Integer.BYTES, Integer.BYTES + Long.BYTES));
        long end = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, Integer.BYTES + Long.BYTES, Integer.BYTES + Long.BYTES + Long.BYTES));
//        long len = end - begin;
//        System.out.println("begin: " + begin + "\tend: " + end + "\tlen: " + len);
        Property property = propertiesBinaryMapper.toProperty(
                Arrays.copyOfRange(bytes, (int) begin, (int) end));
//        System.out.println(property);
        BinaryProperty binaryProperty =
                propertiesBinaryMapper.toBinaryFigure(property);
        binaryProperties.add(binaryProperty);


        for (int i = 1; i < numberOfProperties; i++) {
            begin = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, (int) end, (int) end+ Long.BYTES));
            end = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, (int) end+ Long.BYTES, (int) end+ Long.BYTES + Long.BYTES));
//            len = end - begin;
//            System.out.println("begin: " + begin + "\tend: " + end + "\tlen: " + len);
//            System.out.println("key: " + printKey((int) begin, (int) end));

            property = propertiesBinaryMapper.toProperty(
                    Arrays.copyOfRange(bytes, (int) begin, (int) end));
            binaryProperty =
                    propertiesBinaryMapper.toBinaryFigure(property);
            binaryProperties.add(binaryProperty);
        }
        return binaryProperties;
    }

    public int getNumberOfProperties() {
        return numberOfProperties;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getLength() {
        return bytes.length;
    }

    class PropertyHeader {
        private long beginBinaryPropertySection;
        private long endBinaryPropertySection;
        private BinaryProperty binaryProperty;

        public PropertyHeader(long lastIndex, BinaryProperty binaryProperty) {
            // lastIndex + begin + end
            this.beginBinaryPropertySection = lastIndex + Long.BYTES + Long.BYTES;
            this.endBinaryPropertySection = beginBinaryPropertySection + binaryProperty.getLength();
            this.binaryProperty = binaryProperty;
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
}
