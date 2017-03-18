package org.hajecsdb.graphs.cypher.DFA;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.Result;

public interface DfaAction {
    Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing);
}
