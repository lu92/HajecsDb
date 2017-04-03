package org.hajecsdb.graphs.cypher;

import java.util.HashMap;
import java.util.Map;

public class Result {
    private String command;
    private boolean completed;
    private Map<Integer, ResultRow> results = new HashMap<>();

    public boolean hasContent() {
        return !results.isEmpty();
    }

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

    public Result copy() {
        Result copy = new Result();
        copy.setCommand(command);
        copy.setCompleted(completed);
        for (int i = 0; i < getResults().size(); i++) {

            ResultRow resultRowOrigin = getResults().get(i+1);

            // fill ResultRow copy
            ResultRow resultRowCopy = new ResultRow();
            resultRowCopy.setContentType(resultRowOrigin.getContentType());
            resultRowCopy.setMessage(resultRowOrigin.getMessage());
            resultRowCopy.setNode(resultRowOrigin.getNode());
            resultRowCopy.setRelationship(resultRowOrigin.getRelationship());

            copy.getResults().put(i, resultRowCopy);
        }
        return copy;
    }
}
