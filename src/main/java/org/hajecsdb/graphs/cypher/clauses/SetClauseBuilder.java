package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.cypher.clauses.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.clauses.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.clauses.DFA.State;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SetClauseBuilder extends ClauseBuilder {
    public SetClauseBuilder() {
        super(ClauseEnum.SET);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    String variable = matcher.group(1);
                    String property = matcher.group(2);
                    String value = matcher.group(3);
                    Optional<Property> propertyOptional = transformToProperty(property, value);
                    setOrUpdate(result, propertyOptional.get());
                }
                return result;
            }

            private void setOrUpdate(Result result, Property property) {
                for (Map.Entry<Integer, ResultRow> entry : result.getResults().entrySet()) {
                    Node node = entry.getValue().getNode();
                    if (node.hasProperty(property.getKey())) {
                        node.removeProperty(property.getKey());
                    }
                    node.getAllProperties().add(property);
                }
                ResultRow resultRow = new ResultRow();
                resultRow.setContentType(ContentType.STRING);
                resultRow.setMessage("Properties set: " + result.getResults().size());

                result.getResults().clear();
                result.setCompleted(true);
                result.getResults().put(0, resultRow);
            }
        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "([\\w]+).([\\w]+)=([\\w']+)";
    }
}
