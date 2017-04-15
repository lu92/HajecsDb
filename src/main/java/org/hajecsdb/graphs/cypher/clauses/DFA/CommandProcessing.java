package org.hajecsdb.graphs.cypher.clauses.DFA;

import org.hajecsdb.graphs.cypher.clauses.helpers.QueryContext;

import java.util.Stack;

public class CommandProcessing {
    private String command;
    private Stack<ClauseInvocation> clauseInvocationStack;
    private QueryContext queryContext;

    public CommandProcessing(String command) {
        this.command = command;
        this.queryContext = new QueryContext();
    }

    public String getOriginCommand() {
        return command;
    }

    public Stack<ClauseInvocation> getClauseInvocationStack() {
        return clauseInvocationStack;
    }

    public QueryContext getQueryContext() {
        return queryContext;
    }

    public void setClauseInvocationStack(Stack<ClauseInvocation> clauseInvocationStack) {
        this.clauseInvocationStack = clauseInvocationStack;
    }
}
