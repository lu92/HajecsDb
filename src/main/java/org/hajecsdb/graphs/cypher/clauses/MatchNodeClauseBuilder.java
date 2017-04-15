package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.clauses.DFA.*;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hajecsdb.graphs.cypher.clauses.helpers.ContentType.NODE;

public class MatchNodeClauseBuilder extends ClauseBuilder {

    public MatchNodeClauseBuilder() {
        super(ClauseEnum.MATCH_NODE);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, CommandProcessing commandProcessing) {
                Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
                ClauseInvocation clauseInvocation = commandProcessing.getClauseInvocationStack().peek();
                Matcher matcher = pattern.matcher(clauseInvocation.getSubQuery());
                if (matcher.find()) {
                    String variableName = matcher.group(1);
                    Label label = new Label(matcher.group(2));
                    String parametersBody = matcher.group(3);
                    List<Property> parameters = parameterExtractor.extractParameters(parametersBody);
                    matchNode(graph, result, label, parameters);
                    if (!variableName.isEmpty()) {
                        commandProcessing.getQueryContext().insert(variableName, result.copy());
                    }
                }
                result.setCompleted(true);
                return result;
            }

            void matchNode(Graph graph, Result result, Label label, List<Property> parameters) {
                result.getResults().clear();
                List<Node> filteredNodes = null;

                if (label.getName().isEmpty()) {
                    filteredNodes = graph.getAllNodes().stream().collect(Collectors.toList());
                } else {
                    filteredNodes = graph.getAllNodes().stream()
                            .filter(node -> node.getLabel().equals(label))
                            .collect(Collectors.toList());
                }

                // filter by parameters
                if (!parameters.isEmpty()) {
                    filteredNodes = filteredNodes.stream().filter(node -> node.getAllProperties().getAllProperties().containsAll(parameters))
                            .collect(Collectors.toList());
                }

                List<ResultRow> resultRows = filteredNodes.stream().map(node -> {
                    ResultRow resultRow = new ResultRow();
                    resultRow.setContentType(NODE);
                    resultRow.setNode(node);
                    return resultRow;
                }).collect(Collectors.toList());


                for (int i = 0; i < resultRows.size(); i++) {
                    result.getResults().put(i + 1, resultRows.get(i));
                }
                result.setCompleted(true);
            }

        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "\\(([\\w]+):?([\\w]*)(\\{[\\w:' ,]+\\})?\\)";
    }

    public State buildClause(DFA dfa, State state) {
        State clauseState = state;
        State actionState = new State(clauseEnum, "[" + clauseEnum + "] action state!");
        new Transition(clauseState, actionState, validateClause(), clauseAction());
        new Transition(actionState, actionState, validateClause(), clauseAction());
        return actionState;
    }
}
