package org.hajecsdb.graphs.cypher.clauses;


import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.DFA.State;
import org.hajecsdb.graphs.cypher.Result;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateNodeClauseBuilder extends ClauseBuilder {

    public CreateNodeClauseBuilder(Graph graph) {
        super(ClauseEnum.CREATE_NODE, graph);
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
                    List<Property> parameters = new LinkedList<>();

                    if (matcher.groupCount() == 4 && matcher.group(4) != null) {
                        String paramContent = matcher.group(4);
                        String paramRegex = "([\\w]*):([\\w'.]*)";
                        Pattern paramPattern = Pattern.compile(paramRegex);
                        Matcher paramsMatcher = paramPattern.matcher(paramContent);

                        while (paramsMatcher.find()) {
                            String variable = paramsMatcher.group(1);
                            String value = paramsMatcher.group(2);
                            Property property = parameterExtractor.extract(variable, value);
                            parameters.add(property);
                        }
                    }
                    createNode(graph, result, label, parameters);
                }
                return result;
            }

            void createNode(Graph graph, Result result, Label label, List<Property> parameters) {
                if (parameters.isEmpty()) {
                    graph.createNode(label);
                } else {
                    Properties properties = new Properties();
                    properties.addAll(parameters);
                    graph.createNode(label, properties);
                }
                result.setCompleted(true);
            }

        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "\\(([\\w]*):([\\w]+)( )?(\\{[\\w: ',.]*\\})?\\)";
    }
}
