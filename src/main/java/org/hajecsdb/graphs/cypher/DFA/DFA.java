package org.hajecsdb.graphs.cypher.DFA;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.Query;
import org.hajecsdb.graphs.cypher.Result;

import java.util.LinkedList;
import java.util.List;

public class DFA {
    private List<State> stateList = new LinkedList<>();
    private List<Transition> transitionList = new LinkedList<>();
    private List<Query> queries;
    private State beginState;
    private CommandProcessing commandProcessing;

    public DFA() {
        this.beginState = new State("[Begin state]");
    }

    public Result parse(Graph graph, String command) {
        Result result = new Result();
        result.setCommand(command);
        CommandProcessing commandProcessing = new CommandProcessing(command);
        result = beginState.invoke(graph, result, commandProcessing);
        this.commandProcessing = commandProcessing;
//        return commandProcessing.getQueries();
        return result;
    }

    public State getBeginState() {
        return beginState;
    }

    public void setBeginState(State beginState) {
        this.beginState = beginState;
    }

    public void addStates(List<State> stateList) {
        this.stateList = stateList;
    }

    public void addTransitions(List<Transition> transitionList) {
        this.transitionList = transitionList;
    }

    public CommandProcessing getCommandProcessing() {
        return commandProcessing;
    }
}
