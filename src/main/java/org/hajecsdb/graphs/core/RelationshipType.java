package org.hajecsdb.graphs.core;

public class RelationshipType {
    private String name;

    public RelationshipType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final RelationshipType that = (RelationshipType) o;

        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "RelationshipType{" +
                "name='" + name + '\'' +
                '}';
    }
}
