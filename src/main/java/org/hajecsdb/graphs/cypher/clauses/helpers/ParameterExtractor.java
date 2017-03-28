package org.hajecsdb.graphs.cypher.clauses.helpers;

import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ParameterExtractor {

    public Property extract(String key, String value) {
        Property property;
        if (isStringType(value)) {
            property = new Property(key, PropertyType.STRING, value.substring(1, value.length() - 1));
        } else if (isIntType(value)) {
            property = new Property(key, PropertyType.INT, new Integer(value));
        } else if (isDoubleType(value)) {
            property = new Property(key, PropertyType.DOUBLE, new Double(value));
        } else
            throw new NotImplementedException();

        return property;
    }

    private boolean isStringType(String value) {
        return value.contains("'");
    }

    private boolean isIntType(String value) {
        return value.matches("[\\d]+");
    }

    private boolean isDoubleType(String value) {
        return value.matches("[\\d]+.[\\d]+");
    }
}