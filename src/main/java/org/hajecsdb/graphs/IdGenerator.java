package org.hajecsdb.graphs;

public class IdGenerator {
    private long lastGeneratedIndex = 0;

    public IdGenerator() {
    }

    public IdGenerator(long lastGeneratedIndex) {
        this.lastGeneratedIndex = lastGeneratedIndex;
    }

    public long generateId() {
        return ++lastGeneratedIndex;
    }

    public long getLastId() {
        return lastGeneratedIndex;
    }
}
