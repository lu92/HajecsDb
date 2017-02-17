package org.hajecsdb.graphs.storage.entities;

import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.storage.mappers.PropertiesBinaryMapper;
import org.hajecsdb.graphs.storage.ByteUtils;
import org.hajecsdb.graphs.storage.mappers.PropertyHeaderMapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BinaryProperties {
    int numberOfProperties;
    long beginIndex;
    long lastIndex;
    List<PropertyHeader> propertyHeaderList = new ArrayList<>();
    byte[] bytes;


    private PropertyHeaderMapper propertyHeaderMapper = new PropertyHeaderMapper();

    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

    public BinaryProperties() {
        numberOfProperties = 0;
        beginIndex = 0;
        lastIndex = Integer.BYTES+2*Long.BYTES;
        bytes = ByteBuffer.allocate(Integer.BYTES).putInt(numberOfProperties).array();
    }

    public void addProperty(BinaryProperty binaryProperty) {
        PropertyHeader mappedPropertyHeader = propertyHeaderMapper.mapPropertyHeader(binaryProperty, lastIndex);
        numberOfProperties++;
        propertyHeaderList.add(mappedPropertyHeader);
        lastIndex += mappedPropertyHeader.getLength();
        invalidateBytes(propertyHeaderList);
    }

    private void invalidateBytes(List<PropertyHeader> propertyHeaderList) {
        int totalBytes = Integer.BYTES + Long.BYTES + Long.BYTES + propertyHeaderList.stream()
                .mapToInt(binaryProperty -> binaryProperty.getLength())
                .sum();

        ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes)
                .putInt(numberOfProperties)
                .putLong(beginIndex)
                .putLong(lastIndex);

        propertyHeaderList.stream().forEach(header -> byteBuffer.put(header.getBytes()));
        bytes = byteBuffer.array();
    }

    public List<BinaryProperty> getBinaryProperties() {

        int numberOfProperties = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, 0, Integer.BYTES));

        List<BinaryProperty> binaryProperties = new ArrayList<>(numberOfProperties);
        if (numberOfProperties == 0)
            return binaryProperties;

        int localShift = Integer.BYTES + Long.BYTES + Long.BYTES;
        for (int i=0; i<numberOfProperties; i++) {

            long beginHeader = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, localShift, localShift + Long.BYTES));
            long endHeader = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, localShift + Long.BYTES, localShift + Long.BYTES + Long.BYTES));

//            System.out.println("(" + beginHeader + ", " + endHeader + ")");

            Property property = propertiesBinaryMapper.toProperty(Arrays.copyOfRange(bytes, (int) beginHeader, (int) endHeader));
//            System.out.println(property);

            binaryProperties.add(propertiesBinaryMapper.toBinaryFigure(property));

//            System.out.print("localShift: " + localShift + " -> ");
            localShift += (2*Long.BYTES + endHeader - beginHeader);
//            System.out.println(localShift);

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
}
