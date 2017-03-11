package org.hajecsdb.graphs.cypher.DFA;

import org.hajecsdb.graphs.cypher.Query;

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

    public List<Query> parse(String command) {
        CommandProcessing commandProcessing = new CommandProcessing(command);
        beginState.invoke(commandProcessing);
        this.commandProcessing = commandProcessing;
        return commandProcessing.getQueries();
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
