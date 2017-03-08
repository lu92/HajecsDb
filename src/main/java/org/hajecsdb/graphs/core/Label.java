package org.hajecsdb.graphs.core;

public class Label {
    private String name;

    public Label(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Label label = (Label) o;

        return name.equals(label.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
