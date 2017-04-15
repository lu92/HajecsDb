package org.hajecsdb.graphs.cypher.clauses.DFA;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor.ParameterExtractor;

public interface DfaAction {
    Result perform(Graph graph, Result result, CommandProcessing commandProcessing);

    ParameterExtractor parameterExtractor = new ParameterExtractor();
}
