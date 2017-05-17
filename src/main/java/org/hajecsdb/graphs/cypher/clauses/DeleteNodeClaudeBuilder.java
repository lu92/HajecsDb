package org.hajecsdb.graphs.cypher.clauses;


import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.clauses.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;

import java.util.Map;

public class DeleteNodeClaudeBuilder extends ClauseBuilder {

    public DeleteNodeClaudeBuilder() {
        super(ClauseEnum.DELETE);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(TransactionalGraphService graph, Transaction transaction, Result result, CommandProcessing commandProcessing) {
                int deletedNodes = 0;
                for (Map.Entry<Integer, ResultRow> entry : result.getResults().entrySet()) {
                    ResultRow resultRow = entry.getValue();
                    if (resultRow.getContentType() == ContentType.NODE) {
                        graph.context(transaction).deleteNode(entry.getValue().getNode().getId());
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
