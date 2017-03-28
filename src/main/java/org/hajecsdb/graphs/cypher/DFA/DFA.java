package org.hajecsdb.graphs.cypher.DFA;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.Result;

public class DFA {
    private State beginState;
    private CommandProcessing commandProcessing;

    public DFA() {
        this.beginState = new State(null, "[Begin state]");
    }

    public Result parse(Graph graph, CommandProcessing commandProcessing) {
        Result result = new Result();
        result.setCommand(commandProcessing.getOriginCommand());
        result = beginState.invoke(graph, result, commandProcessing);
        this.commandProcessing = commandProcessing;
        return result;
    }

    public State getBeginState() {
        return beginState;
    }

    public CommandProcessing getCommandProcessing() {
        return commandProcessing;
    }
}
