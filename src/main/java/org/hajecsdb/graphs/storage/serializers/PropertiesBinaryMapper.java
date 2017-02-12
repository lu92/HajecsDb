package org.hajecsdb.graphs.storage.serializers;

import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.storage.ByteUtils;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;
import org.hajecsdb.graphs.storage.entities.BinaryProperty;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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

    public Properties toProperties(BinaryProperties binaryProperties) {
        throw new UnsupportedOperationException();
    }

    public Properties toProperties(byte [] binaryProperties) {
        throw new UnsupportedOperationException();
    }

    public BinaryProperties convertBinaryFigureIntoProperties(Properties properties) {

        return null;
    }

}
