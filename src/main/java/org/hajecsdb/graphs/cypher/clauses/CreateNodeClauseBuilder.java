package org.hajecsdb.graphs.cypher.clauses;


import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.DFA.State;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateNodeClauseBuilder extends ClauseBuilder {

    public CreateNodeClauseBuilder() {
        super(ClauseEnum.CREATE_NODE);
    }

    @Override
    public DfaAction clauseAction() {

        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    String variableName = matcher.group(1);
                    Label label = new Label(matcher.group(2));
                    String parametersBody = matcher.group(4);
                    List<Property> parameters = extractParameters(parametersBody);
                    createNode(graph, result, label, parameters);
                    commandProcessing.getQueryContext().insert(variableName, result.copy());
                }
                return result;
            }

            void createNode(Graph graph, Result result, Label label, List<Property> parameters) {
                Node node = createNode(graph, label, parameters);
                int index = result.getResults().size() + 1;
                ResultRow resultRow = new ResultRow();
                result.setCompleted(true);
                resultRow.setContentType(ContentType.NODE);
                resultRow.setNode(node);
                result.getResults().put(index, resultRow);
            }

            private Node createNode(Graph graph, Label label, List<Property> parameters) {
                if (parameters.isEmpty()) {
                    return graph.createNode(label);
                } else {
                    Properties properties = new Properties();
                    properties.addAll(parameters);
                    return graph.createNode(label, properties);
                }
            }

        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "\\(([\\w]*):([\\w]+)( )?(\\{[\\w: ',.]*\\})?\\)";
    }
}
