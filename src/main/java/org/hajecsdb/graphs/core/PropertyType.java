package org.hajecsdb.graphs.core;

public enum PropertyType {
    NONE(0),
    INT(1),
    LONG(2),
    FLOAT(3),
    DOUBLE(4),
    STRING(5),
    DATE(6),
    TIME(7),
    DATE_TIME(8);

    PropertyType(int binaryCode) {
        this.binaryCode = binaryCode;
    }

    private int binaryCode;

    public int getBinaryCode() {
        return binaryCode;
    }

    public static PropertyType valueOf(byte binaryCode) {
        int code = (int) binaryCode;
        return valueOf(code);
    }

    public static PropertyType valueOf(int binaryCode) {
        for (PropertyType property : values()) {
            if (property.binaryCode == binaryCode)
                return property;
        }
        throw new IllegalArgumentException("cannot match given binary code to PropertyType!");
    }
}
