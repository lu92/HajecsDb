package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.cypher.Result;

import java.util.HashMap;
import java.util.Map;

public class QueryContext {
    private Map<String, Result> context = new HashMap<>();

    public void insert(String variableName, Result result) {
        context.put(variableName, result);
    }

    public Result get(String variableName) {
        return context.get(variableName);
    }
}
