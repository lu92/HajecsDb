package org.hajecsdb.graphs.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.DFA.ClausesSeparator;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DFA;

import java.util.Stack;

public class CypherExecutor {
    private Graph graph;
    private CypherDfaBuilder cypherDfaBuilder;
    private ClausesSeparator clausesSeparator;

    public CypherExecutor(Graph graph) {
        this.graph = graph;
        this.cypherDfaBuilder = new CypherDfaBuilder(graph);
        this.clausesSeparator = new ClausesSeparator();
        this.cypherDfaBuilder.buildClauses();
    }

    //        MATCH (neo:Database {name:"Neo4j"})
    //        MATCH (anna:Person {name:"Anna"})
    //        CREATE_NODE (anna)-[:FRIEND]->(:Person:Expert {name:"Amanda"})-[:WORKED_WITH]->(neo)


    public Result execute(String command) {

        DFA dfa = cypherDfaBuilder.getDfa();

        // parse command
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);
        CommandProcessing commandProcessing = new CommandProcessing(command);
        commandProcessing.setClauseInvocationStack(clauseInvocationStack);
        Result result = dfa.parse(graph, commandProcessing);
        return result;
    }
}
