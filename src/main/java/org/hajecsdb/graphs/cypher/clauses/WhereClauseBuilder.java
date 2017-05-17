package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.clauses.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.clauses.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.cypher.clauses.helpers.equationResolver.ArithmeticOperator;
import org.hajecsdb.graphs.cypher.clauses.helpers.equationResolver.Equation;
import org.hajecsdb.graphs.cypher.clauses.helpers.equationResolver.EquationResolver;
import org.hajecsdb.graphs.cypher.clauses.helpers.equationResolver.LogicalOperator;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hajecsdb.graphs.cypher.clauses.helpers.ContentType.NODE;

public class WhereClauseBuilder extends ClauseBuilder {

    public WhereClauseBuilder() {
        super(ClauseEnum.WHERE);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {

            private String getIdentityFunctionRegex() {
                return "id\\(([\\w+]+)\\)=(\\d+)";
            }

            private String getConditionFunctionRegex() {
                return "([\\w]+).([\\w]+)([=><]+)([0-9]+|'[a-zA-Z]*')(AND|OR)?([\\w]+)?.?([\\w]+)?([=><]+)?([\\w']+)?";
            }

            @Override
            public Result perform(TransactionalGraphService graph, Transaction transaction, Result result, CommandProcessing commandProcessing) {
                ClauseInvocation clauseInvocation = commandProcessing.getClauseInvocationStack().peek();
                if (determineExpressionOfSubQuery(clauseInvocation.getSubQuery())) {
                    return performClauseWithIdFunction(result, commandProcessing);
                } else {
                    return performClauseWithConditionFunction(result, commandProcessing);
                }
            }

            private boolean determineExpressionOfSubQuery(String subQuery) {
                boolean identityFunctionMatched = subQuery.matches(getIdentityFunctionRegex());
                boolean conditionFunctionMatched = subQuery.matches(getConditionFunctionRegex());
                if ((identityFunctionMatched || conditionFunctionMatched) == false) {
                    throw new IllegalArgumentException("[WHERE] Any matched function!");
                }
                return identityFunctionMatched;
            }

            private Result performClauseWithConditionFunction(Result result, CommandProcessing commandProcessing) {
                Map<Integer, ResultRow> matchedNodes = new HashMap<>();
                Pattern pattern = Pattern.compile(getConditionFunctionRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    String variable = matcher.group(1);
                    String property = matcher.group(2);
                    String operator = matcher.group(3);
                    String value = matcher.group(4);

                    String logicalOperatorSymbol = matcher.group(5);
                    String variable2 = matcher.group(6);
                    String property2 = matcher.group(7);
                    String operator2 = matcher.group(8);
                    String value2 = matcher.group(9);

                    List<Node> nodeList = nodesWithProperty(result, property);

                    Optional<Property> argumentB = transformToProperty(property, value);
                    Optional<Property> argumentC = transformToProperty(property2, value2);

                    int index = 0;
                    for (Node node : nodeList) {
                        Optional<Property> argumentA = node.getProperty(property);

                        ArithmeticOperator arithmeticOperator = ArithmeticOperator.getOperator(operator);

                        Equation equation1 = new Equation(argumentA, arithmeticOperator, argumentB);

                        ArithmeticOperator arithmeticOperator2 = ArithmeticOperator.getOperator(operator2);
                        LogicalOperator logicalOperator = LogicalOperator.getOperator(logicalOperatorSymbol);
                        Equation equation2 = new Equation(argumentA, arithmeticOperator2, argumentC);

                        EquationResolver equationResolver = new EquationResolver(equation1, logicalOperator, equation2);
                        if (equationResolver.validate()) {
                            ResultRow resultRow = new ResultRow();
                            resultRow.setContentType(NODE);
                            resultRow.setNode(node);
                            matchedNodes.put(index, resultRow);
                            index++;
                        }
                    }
                }

                result.getResults().clear();
                result.getResults().putAll(matchedNodes);
                return result;
            }

            private Result performClauseWithIdFunction(Result result, CommandProcessing commandProcessing) {
                Result finalResult = new Result();
                Pattern pattern = Pattern.compile(getIdentityFunctionRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    String variableName = "id";
                    String argument = matcher.group(2);


                    int index = 1;
                    for (Map.Entry<Integer, ResultRow> entry : result.getResults().entrySet()) {
                        if (entry.getValue().getContentType() == NODE) {
                            if (entry.getValue().getNode().getProperty(variableName).isPresent()) {

                                Property property = entry.getValue().getNode().getProperty(variableName).get();
                                PropertyType argumentType = getArgumentType(argument);

                                if (property.getType().equals(argumentType)) {

                                    switch (argumentType) {
                                        case LONG:
                                            if ((long) property.getValue() == Integer.valueOf(argument)) {
                                                ResultRow resultRow = new ResultRow();
                                                resultRow.setContentType(NODE);
                                                resultRow.setNode(entry.getValue().getNode());
                                                finalResult.getResults().put(index, resultRow);
                                            }
                                            break;
                                    }
                                }
                            }
                        }
                    }
                }

                result.getResults().clear();
                result.getResults().putAll(finalResult.getResults());
                return finalResult;
            }

            private List<Node> nodesWithProperty(Result result, String property) {
                return result.getResults().entrySet().stream().map(entry -> entry.getValue().getNode())
                        .filter(node -> node.getAllProperties().hasProperty(property))
                        .collect(Collectors.toList());
            }
        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "(id\\(([\\w+]+)\\)=(\\d+)|([\\w]+).([\\w]+)([=><]+)([0-9]+|'[a-zA-Z]*')(AND|OR)?([\\w]+)?.?([\\w]+)?([=><]+)?([\\w']+)?)";
    }
}
