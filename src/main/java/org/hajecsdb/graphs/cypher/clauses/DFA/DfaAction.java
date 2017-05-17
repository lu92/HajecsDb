package org.hajecsdb.graphs.cypher.clauses.DFA;

import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor.ParameterExtractor;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;

public interface DfaAction {
    Result perform(TransactionalGraphService graph, Transaction transaction, Result result, CommandProcessing commandProcessing);

    ParameterExtractor parameterExtractor = new ParameterExtractor();
}
