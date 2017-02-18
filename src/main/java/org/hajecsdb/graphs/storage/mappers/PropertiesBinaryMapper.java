package org.hajecsdb.graphs.storage.mappers;

import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.storage.ByteUtils;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;
import org.hajecsdb.graphs.storage.entities.BinaryProperty;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class PropertiesBinaryMapper {
    public static final int KEY_SIZE = 30;

    public BinaryProperty toBinaryFigure(Property property) {
        byte[] key = ByteBuffer.allocate(KEY_SIZE).put(property.getKey().getBytes()).array();
        byte type = (byte) property.getType().getBinaryCode();;
        byte[] value;
        switch (property.getType()) {
            case NONE:
                return null;

            case INT:
                value = ByteBuffer.allocate(Integer.BYTES).putInt((int) property.getValue()).array();
                return new BinaryProperty(key, type, value);

            case LONG:
                value = ByteBuffer.allocate(Long.BYTES).putLong((long) property.getValue()).array();
                return new BinaryProperty(key, type, value);

            case FLOAT:
                value = ByteBuffer.allocate(Float.BYTES).putFloat((float) property.getValue()).array();
                return new BinaryProperty(key, type, value);

            case DOUBLE:
                value = ByteBuffer.allocate(Double.BYTES).putDouble((double) property.getValue()).array();
                return new BinaryProperty(key, type, value);

            case STRING:
                String stringValue = (String) property.getValue();
                value = ByteBuffer.allocate(stringValue.getBytes().length).put((stringValue).getBytes()).array();
                return new BinaryProperty(key, type, value);

            case DATE:
                throw new UnsupportedOperationException();

            case TIME:
                throw new UnsupportedOperationException();

            case DATE_TIME:
                throw new UnsupportedOperationException();

            default:
                return null;
        }
    }

    public Property fromBinaryFigure(byte [] bytes) {
        String key = ByteUtils.bytesToString(ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, KEY_SIZE)).array());
        PropertyType propertyType = PropertyType.valueOf(Arrays.copyOfRange(bytes, KEY_SIZE, KEY_SIZE + 1)[0]);
        switch (propertyType) {
            case NONE:
                return null;

            case INT:
                int intValue = ByteUtils.bytesToInt(ByteBuffer.wrap(Arrays.copyOfRange(bytes, KEY_SIZE + 1, bytes.length)).array());
                return new Property(key, propertyType, intValue);

            case LONG:
                long longValue = ByteUtils.bytesToLong(ByteBuffer.wrap(Arrays.copyOfRange(bytes, KEY_SIZE + 1, bytes.length)).array());
                return new Property(key, propertyType, longValue);

            case FLOAT:
                float floatValue = ByteUtils.bytesToFloat(ByteBuffer.wrap(Arrays.copyOfRange(bytes, KEY_SIZE + 1, bytes.length)).array());
                return new Property(key, propertyType, floatValue);

            case DOUBLE:
                double doubleValue = ByteUtils.bytesToDouble(ByteBuffer.wrap(Arrays.copyOfRange(bytes, KEY_SIZE + 1, bytes.length)).array());
                return new Property(key, propertyType, doubleValue);

            case STRING:
                String stringValue = ByteUtils.bytesToString(ByteBuffer.wrap(Arrays.copyOfRange(bytes, KEY_SIZE + 1, bytes.length)).array());
                return new Property(key, propertyType, stringValue);

            case DATE:
                throw new UnsupportedOperationException();

            case TIME:
                throw new UnsupportedOperationException();

            case DATE_TIME:
                throw new UnsupportedOperationException();

            default:
                return null;
        }
    }

    public BinaryProperties toBinaryFigure(Properties properties) {
        BinaryProperties binaryProperties = new BinaryProperties();
        for (Property property : properties.getAllProperties()) {
            BinaryProperty binaryProperty = toBinaryFigure(property);
            binaryProperties.addProperty(binaryProperty);
        }
        return binaryProperties;
    }


    public Property toProperty(byte[] bytes) {
        String key = ByteUtils.bytesToString(Arrays.copyOfRange(bytes, 0, KEY_SIZE));
        PropertyType type = PropertyType.valueOf((int)Arrays.copyOfRange(bytes, KEY_SIZE, KEY_SIZE+1)[0]);
        byte[] value = Arrays.copyOfRange(bytes, KEY_SIZE+1, bytes.length);
        switch (type) {
            case INT:
                return new Property(key, type, ByteUtils.bytesToInt(value));

            case LONG:
                return new Property(key, type, ByteUtils.bytesToLong(value));

            case FLOAT:
                return new Property(key, type, ByteUtils.bytesToFloat(value));

            case DOUBLE:
                return new Property(key, type, ByteUtils.bytesToDouble(value));

            case STRING:
                return new Property(key, type, ByteUtils.bytesToString(value));

            case DATE:
                throw new UnsupportedOperationException();

            case TIME:
                throw new UnsupportedOperationException();

            case DATE_TIME:
                throw new UnsupportedOperationException();

            default:
                throw new IllegalArgumentException("cannot recognize type of property!");
        }
    }

    public Properties toProperties(byte [] bytes) {
        BinaryProperties binaryProperties = fromBinaryFigureToBinaryProperties(bytes);
        return toProperties(binaryProperties);
    }

    public Properties toProperties(BinaryProperties binaryProperties) {
        Properties properties = new Properties();
        for (BinaryProperty binaryProperty : binaryProperties.getBinaryProperties()) {
            properties.add(fromBinaryFigure(binaryProperty.getBytes()));
        }
        return properties;
    }

    public BinaryProperties fromBinaryFigureToBinaryProperties(byte [] bytes) {
        int numberOfProperties = ByteUtils.bytesToInt(ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, Integer.BYTES)).array());
        BinaryProperties binaryProperties = new BinaryProperties();
        int localShift = Integer.BYTES + Long.BYTES + Long.BYTES;
        for (int i=0; i<numberOfProperties; i++) {
            long beginHeader = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, localShift, localShift + Long.BYTES));
            long endHeader = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, localShift + Long.BYTES, localShift + Long.BYTES + Long.BYTES));
            Property property = this.toProperty(Arrays.copyOfRange(bytes, (int) beginHeader, (int) endHeader));
            binaryProperties.addProperty(this.toBinaryFigure(property));
            localShift += (2*Long.BYTES + endHeader - beginHeader);
        }
        return binaryProperties;
    }

    public BinaryProperties convertBinaryFigureIntoProperties(Properties properties) {

        return null;
    }

}
