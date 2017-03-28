package org.hajecsdb.graphs.cypher.DFA;

import java.util.Stack;

public class CommandProcessing {
    private String command;
    private String commandToProceed;
    private Stack<ClauseInvocation> clauseInvocationStack;

    public CommandProcessing(String command) {
        this.command = command;
        this.commandToProceed = command;
    }

    public String getOriginCommand() {
        return command;
    }

    public String getProcessingCommand() {
        return commandToProceed;
    }

    public Stack<ClauseInvocation> getClauseInvocationStack() {
        return clauseInvocationStack;
    }

    public void setClauseInvocationStack(Stack<ClauseInvocation> clauseInvocationStack) {
        this.clauseInvocationStack = clauseInvocationStack;
    }
}
