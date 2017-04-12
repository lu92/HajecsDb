package org.hajecsdb.graphs.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.DFA.ClausesSeparator;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DFA;

import java.util.Stack;

public final class CypherExecutor {
    private CypherDfaBuilder cypherDfaBuilder;
    private ClausesSeparator clausesSeparator;

    public CypherExecutor() {
        this.cypherDfaBuilder = new CypherDfaBuilder();
        this.clausesSeparator = new ClausesSeparator();
        this.cypherDfaBuilder.buildClauses();
    }

    public Result execute(Graph graph, String command) {
        DFA dfa = cypherDfaBuilder.getDfa();
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);
        CommandProcessing commandProcessing = new CommandProcessing(command);
        commandProcessing.setClauseInvocationStack(clauseInvocationStack);
        Result result = dfa.parse(graph, commandProcessing);
        return result;
    }
}
