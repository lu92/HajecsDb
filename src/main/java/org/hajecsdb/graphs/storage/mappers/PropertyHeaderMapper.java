package org.hajecsdb.graphs.storage.mappers;


import org.hajecsdb.graphs.storage.entities.BinaryProperty;
import org.hajecsdb.graphs.storage.entities.PropertyHeader;

import java.util.ArrayList;
import java.util.List;

public class PropertyHeaderMapper {

    public PropertyHeader mapPropertyHeader(BinaryProperty binaryProperty, long position) {
        long begin = (position+2*Long.BYTES);
        long end = (begin + binaryProperty.getLength());
        return new PropertyHeader(begin, end, binaryProperty);
    }

    public PropertyHeader mapPropertyHeader(BinaryProperty binaryProperty) {
        return mapPropertyHeader(binaryProperty, 0);
    }

    public List<PropertyHeader> mapPropertyHeaders(List<BinaryProperty> binaryPropertyList, long shift) {
        List<PropertyHeader> propertyHeaders = new ArrayList<>(binaryPropertyList.size());
        for (BinaryProperty binaryProperty : binaryPropertyList) {
            PropertyHeader propertyHeader = mapPropertyHeader(binaryProperty, shift);
            propertyHeaders.add(propertyHeader);
            shift += propertyHeader.getLength();
        }
        return propertyHeaders;
    }

    public List<PropertyHeader> mapPropertyHeaders(List<BinaryProperty> binaryPropertyList) {
        return mapPropertyHeaders(binaryPropertyList, 0);
    }
}
