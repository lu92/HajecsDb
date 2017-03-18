package org.hajecsdb.graphs.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.cypher.DFA.DFA;

import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.hajecsdb.graphs.cypher.ContentType.NODE;

public class CypherExecutor {
    private Graph graph;
    private CypherDfaBuilder cypherDfaBuilder;
//    private Stack<Query> queries = new Stack<>();

    public CypherExecutor(Graph graph) {
        this.graph = graph;
        cypherDfaBuilder = new CypherDfaBuilder(graph);
        this.cypherDfaBuilder.buildClauses();
    }

    //        MATCH (neo:Database {name:"Neo4j"})
    //        MATCH (anna:Person {name:"Anna"})
    //        CREATE_NODE (anna)-[:FRIEND]->(:Person:Expert {name:"Amanda"})-[:WORKED_WITH]->(neo)


    public Result execute(String command) {


        // parse command
        DFA dfa = cypherDfaBuilder.getDfa();
        Result result = dfa.parse(graph, command);

//        // execute query
//        for (Query query : queries) {
//            switch (query.getCommandType()) {
//                case CREATE_NODE:
////                    createNode(query, result);
//                    break;
//
//                case MATCH_NODE:
//                    matchNode(query, result);
//                    break;
//
//                case WHERE:
//                    whereExpression(query, result);
//                    break;
//
//                default:
//                    throw new IllegalArgumentException("not implemented clause!");
//            }
//        }

        // clear queries stack

        return result;
    }

//    void createNode(Query query, Result result) {
//        if (query.getParameters().isEmpty()) {
//            graph.createNode(query.getLabel());
//        } else {
//            Properties properties = new Properties();
//            properties.addAll(query.getParameters());
//            graph.createNode(query.getLabel(), properties);
//        }
//        result.setCompleted(true);
//    }
//
//    void matchNode(Query query, Result result) {
//        List<Node> filteredNodesByLabel = null;
//
//        if (query.getLabel().getName().isEmpty()) {
//            filteredNodesByLabel = graph.getAllNodes().stream().collect(Collectors.toList());
//        } else {
//            filteredNodesByLabel = graph.getAllNodes().stream()
//                    .filter(node -> node.getLabel().equals(query.getLabel()))
//                    .collect(Collectors.toList());
//        }
//        for (int i = 0; i < filteredNodesByLabel.size(); i++) {
//            ResultRow resultRow = new ResultRow();
//            resultRow.setContentType(NODE);
//            resultRow.setNode(filteredNodesByLabel.get(i));
//        }
//
//        List<ResultRow> resultRows = filteredNodesByLabel.stream().map(node -> {
//            ResultRow resultRow = new ResultRow();
//            resultRow.setContentType(NODE);
//            resultRow.setNode(node);
//            return resultRow;
//        }).collect(Collectors.toList());
//
//
//        for (int i = 0; i < resultRows.size(); i++) {
//            result.getResults().put(i + 1, resultRows.get(i));
//        }
//        result.setCompleted(true);
//    }
//
//    void whereExpression(Query query, Result result) {
//
//    }
}
