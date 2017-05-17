package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.clauses.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hajecsdb.graphs.core.PropertyType.STRING;

public class RemoveClauseBuilder extends ClauseBuilder {

    public RemoveClauseBuilder() {
        super(ClauseEnum.REMOVE);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(TransactionalGraphService graph, Transaction transaction, Result result, CommandProcessing commandProcessing) {
                Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    String variable = matcher.group(1);
                    String operator = matcher.group(2);
                    String propertyName = matcher.group(3);
                    if (operator.equals(":")) {
                        Property property = new Property("label", STRING, propertyName);
                        removeLabel(result, property);
                    } else {
                        removeProperty(result, propertyName);
                    }
                }
                return result;
            }

            void removeLabel(Result result, Property property) {
                int index = 0;
                for (Map.Entry<Integer, ResultRow> entry : result.getResults().entrySet()) {
                    ResultRow resultRow = entry.getValue();
                    if (resultRow.getContentType() == ContentType.NODE &&
                            resultRow.getNode().getLabel().getName().equals((String) property.getValue())) {
                        resultRow.getNode().getAllProperties().delete("label");
                        index++;
                    }
                }
                ResultRow resultRow = new ResultRow();
                resultRow.setContentType(ContentType.STRING);
                resultRow.setMessage("Labels removed: " + index);

                result.setCompleted(true);
                result.getResults().clear();
                result.getResults().put(0, resultRow);
            }

            void removeProperty(Result result, String property) {
                int index = 0;
                for (Map.Entry<Integer, ResultRow> entry : result.getResults().entrySet()) {
                    ResultRow resultRow = entry.getValue();
                    if (resultRow.getContentType() == ContentType.NODE && resultRow.getNode().hasProperty(property)) {
                        resultRow.getNode().getAllProperties().delete(property);
                        index++;
                    }
                }
                ResultRow resultRow = new ResultRow();
                resultRow.setContentType(ContentType.STRING);
                resultRow.setMessage("Properties removed: " + index);

                result.setCompleted(true);
                result.getResults().clear();
                result.getResults().put(0, resultRow);
            }
        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "([\\w]+)(:|.)([\\w]+)";
    }
}
