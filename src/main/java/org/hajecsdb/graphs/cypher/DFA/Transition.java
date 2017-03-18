package org.hajecsdb.graphs.cypher.DFA;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.Result;

import java.util.function.Predicate;

public class Transition {
    private State beginState;
    private State nextState;
    private Predicate<String> matched;
    private DfaAction action;

    public Transition(State beginState, State nextState, Predicate<String> matched, DfaAction action) {
        this.beginState = beginState;
        this.nextState = nextState;
        this.matched = matched;
        this.action = action;
        this.beginState.getOutgoingTransitionList().add(this);
        this.nextState.getIncomingTransitionList().add(this);
    }

    public boolean isMatched(String command) {
        return matched.test(command);
    }

    public Result performAction(Graph graph, Result result, CommandProcessing commandProcessing) {
        return action.perform(graph, result, beginState, commandProcessing);
    }

    public State getNextState() {
        return nextState;
    }

    @Override
    public String toString() {
        return "Transition{" +
                "beginState=" + beginState.getDescription() +
                ", nextState=" + nextState.getDescription() +
                '}';
    }
}
