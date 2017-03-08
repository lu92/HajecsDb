package org.hajecsdb.graphs.cypher.DFA;

public interface DfaAction {
    void perform(State currentState, CommandProcessing commandProcessing);
}
