package org.hajecsdb.graphs.storage.entities;


import org.apache.commons.lang3.ArrayUtils;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.storage.ByteUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;


public class BinaryProperty {
    public static final int KEY_SIZE = 30;
    public static final int TYPE_SIZE = 8;
    private byte[] key;
    private byte[] type;
    private byte[] bytes;
    private PropertyType propertyType;

    public BinaryProperty(byte[] key, byte[] value, byte[] type) {
        this.key = key;
        this.type = type;
        this.bytes = mergePropertyIntoByteArray(key, value, type);
        this.propertyType = PropertyType.valueOf(ByteBuffer.wrap(type).getInt());
    }

    private byte[] mergePropertyIntoByteArray(byte [] key, byte [] value, byte [] type) {
        return ArrayUtils.addAll(ArrayUtils.addAll(key, value), type);
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
        return Arrays.copyOfRange(bytes, KEY_SIZE, bytes.length-TYPE_SIZE);
    }

    public byte[] getBinaryType() {
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
        return "BinaryProperty{" + getKey() + ", " + getValue() + ", " + getType() + "}";
    }
}
