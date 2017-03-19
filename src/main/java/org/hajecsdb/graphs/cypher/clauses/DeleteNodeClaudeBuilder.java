package org.hajecsdb.graphs.cypher.clauses;


import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.Map;
import java.util.function.Predicate;

public class DeleteNodeClaudeBuilder extends ClauseBuilder {

    public DeleteNodeClaudeBuilder(Graph graph) {
        super(graph);
    }

    @Override
    public State buildClause(DFA dfa, State state) {

        State verifyDeleteClause = state;
        State performDeletePart = new State("[DELETE] extract conditions!");
        State endState = new State("get processed Data!");

        Predicate<String> whereClausePredicate = x -> x.startsWith("DELETE ");


        DfaAction extractNodePartAction = new DfaAction() {

            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                commandProcessing.recordProcessedPart("DELETE ", currentState);
//                String commandToProcessed = commandProcessing.getProcessingCommand().substring(7);
                commandProcessing.updateCommand("");

                int deletedNodes = 0;
                for (Map.Entry<Integer, ResultRow> entry : result.getResults().entrySet()) {
                    ResultRow resultRow = entry.getValue();
                    if (resultRow.getContentType() == ContentType.NODE) {
                        graph.deleteNode(entry.getValue().getNode().getId());
                        deletedNodes++;
                    }
                }
                result.getResults().clear();

                ResultRow resultRow = new ResultRow();
                result.setCompleted(true);
                resultRow.setContentType(ContentType.STRING);
                resultRow.setMessage("Nodes deleted: " + deletedNodes);
                result.getResults().put(0, resultRow);
                return result;
            }
        };

        Transition matchClauseTransition = new Transition(verifyDeleteClause, performDeletePart, whereClausePredicate, extractNodePartAction);

        return endState;
    }
}
