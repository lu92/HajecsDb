package org.hajecsdb.graphs.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClausesSeparator;
import org.hajecsdb.graphs.cypher.clauses.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.clauses.DFA.DFA;
import org.springframework.stereotype.Component;

import java.util.Stack;

@Component
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
