package org.hajecsdb.graphs.cypher.clauses;


import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.DFA.State;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.Map;

public class DeleteNodeClaudeBuilder extends ClauseBuilder {

    public DeleteNodeClaudeBuilder(Graph graph) {
        super(ClauseEnum.DELETE, graph);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
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
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "[\\w]+";
    }
}
