package org.hajecsdb.graphs.core;

public class Property {
    private String key;
    private PropertyType type;
    private Object value;

    public Property(String key, PropertyType type, Object value) {
        this.key = key;
        this.type = type;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public PropertyType getType() {
        return type;
    }

    public Property copy() {
        return new Property(key,  type, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Property property = (Property) o;

        if (!key.equals(property.key)) return false;
        if (!value.equals(property.value)) return false;
        return type == property.type;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Property{" +
                "key='" + key + '\'' +
                ", type=" + type +
                ", value=" + value +
                '}';
    }
}
