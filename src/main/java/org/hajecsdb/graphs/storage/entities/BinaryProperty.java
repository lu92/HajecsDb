package org.hajecsdb.graphs.storage.entities;


import org.apache.commons.lang3.ArrayUtils;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.storage.ByteUtils;

import java.util.Arrays;


public class BinaryProperty {
    private static final int KEY_SIZE = 30;
    private byte[] key;
    private byte type;
    private byte[] bytes;
    private PropertyType propertyType;

    public BinaryProperty(byte[] key, byte type, byte[] value) {
        this.key = key;
        this.type = type;
        this.bytes = mergePropertyIntoByteArray(key, type, value);
        this.propertyType = PropertyType.valueOf((int) type);
    }

    private byte[] mergePropertyIntoByteArray(byte [] key, byte type, byte [] value) {
        return ArrayUtils.addAll(ArrayUtils.addAll(key, type), value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final BinaryProperty that = (BinaryProperty) o;

        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    public byte[] getBinaryKey() {
        return key;
    }

    public byte[] getBinaryValue() {
        return Arrays.copyOfRange(bytes, KEY_SIZE+1, bytes.length);
    }

    public byte getBinaryType() {
        return type;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public String getKey() {
        return ByteUtils.bytesToString(key);
    }

    public Object getValue() {
        switch (propertyType) {
            case INT:
                return ByteUtils.bytesToInt(getBinaryValue());

            case LONG:
                return ByteUtils.bytesToLong(getBinaryValue());

            case FLOAT:
                return ByteUtils.bytesToFloat(getBinaryValue());

            case DOUBLE:
                return ByteUtils.bytesToDouble(getBinaryValue());

            case STRING:
                return ByteUtils.bytesToString(getBinaryValue());

            default:
                return null;
        }
    }

    public int getLength() {
        return getBytes().length;
    }

    public PropertyType getType() {
        return propertyType;
    }

    @Override
    public String toString() {
        return "BinaryProperty{" + getKey() + ", "  + getType() + ", " + getValue() + "}";
    }
}
