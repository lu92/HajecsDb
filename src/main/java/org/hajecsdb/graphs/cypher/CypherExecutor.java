package org.hajecsdb.graphs.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.cypher.DFA.DFA;

import java.util.List;
import java.util.Stack;

public class CypherExecutor {
    private Graph graph;
    private CypherDfaBuilder cypherDfaBuilder = new CypherDfaBuilder();
    private Stack<Query> queries = new Stack<>();

    public CypherExecutor(Graph graph) {
        this.graph = graph;
        this.cypherDfaBuilder.buildClauses();
    }

    //        MATCH (neo:Database {name:"Neo4j"})
    //        MATCH (anna:Person {name:"Anna"})
    //        CREATE_NODE (anna)-[:FRIEND]->(:Person:Expert {name:"Amanda"})-[:WORKED_WITH]->(neo)


    public Result execute(String command) {
        Result result = new Result();
        result.setCommand(command);

        // parse command
        DFA dfa = cypherDfaBuilder.getDfa();
        List<Query> queries = dfa.parse(command);

        // execute query
        for (Query query : queries) {
            switch (query.getCommandType()) {
                case CREATE_NODE:
                    createNode(query, result);
                    break;

                default:
                    throw new IllegalArgumentException("not implemented clause!");
            }
        }

        // clear queries stack

        return result;
    }

    void createNode(Query query, Result result) {
        if (query.getParameters().isEmpty()) {
            graph.createNode(query.getLabel());
        } else {
            Properties properties = new Properties();
            properties.addAll(query.getParameters());
            graph.createNode(query.getLabel(), properties);
        }
        result.setCompleted(true);
    }
}
