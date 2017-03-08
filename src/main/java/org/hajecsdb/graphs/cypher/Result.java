package org.hajecsdb.graphs.cypher;

import java.util.HashMap;
import java.util.Map;

public class Result {
    private String command;
    private boolean completed;
    private Map<Integer, ResultRow> results = new HashMap<>();

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public Map<Integer, ResultRow> getResults() {
        return results;
    }
}
