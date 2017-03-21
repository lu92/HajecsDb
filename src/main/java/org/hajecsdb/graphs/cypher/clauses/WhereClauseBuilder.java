package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.ArithmeticOperator;
import org.hajecsdb.graphs.cypher.LogicalOperator;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;
import static org.hajecsdb.graphs.cypher.ContentType.NODE;

public class WhereClauseBuilder extends ClauseBuilder{

    public WhereClauseBuilder(Graph graph) {
        super(graph);
    }

    @Override
    public State buildClause(DFA dfa, State state) {
        List<State> stateList = new LinkedList<>();
        List<Transition> transitionList = new LinkedList<>();
        State verifyWhereClause = state;
        State extractConditionsPart = new State("[WHERE] extract conditions!");
        State endState = new State("get processed Data!");

        Predicate<String> whereClausePredicate = x -> x.startsWith("WHERE ");


        DfaAction extractNodePartAction = new DfaAction() {

            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                commandProcessing.recordProcessedPart("WHERE ", currentState);
                String commandToProcessed = commandProcessing.getProcessingCommand().substring(6);
                commandProcessing.updateCommand(commandToProcessed);
                return result;
            }
        };

        Transition matchClauseTransition = new Transition(verifyWhereClause, extractConditionsPart, whereClausePredicate, extractNodePartAction);


        DfaAction identityFuncAction = new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                System.out.println("id func: " + commandProcessing.getProcessingCommand());
                Result finalResult = new Result();

                String identityFunctionRegex = "id\\(([\\w+]+)\\) = (\\d+)";
                Pattern pattern = Pattern.compile(identityFunctionRegex);
                Matcher matcher = pattern.matcher(commandProcessing.getProcessingCommand());
                if (matcher.find()) {
//                    String variableName = matcher.group(1);
                    String variableName = "id";
                    String argument = matcher.group(2);
                    System.out.println(variableName + " : " + argument);


                    int index = 1;
                    for (Map.Entry<Integer, ResultRow> entry : result.getResults().entrySet()) {
                        if (entry.getValue().getContentType() == NODE) {
                            if (entry.getValue().getNode().getProperty(variableName).isPresent()) {

                                Property property = entry.getValue().getNode().getProperty(variableName).get();
                                PropertyType argumentType = getArgumentType(argument);

                                if (property.getType().equals(argumentType)) {

                                    switch (argumentType) {
                                        case LONG:
                                            System.out.println("LONG TYPE");
                                            if ((long)property.getValue() == Integer.valueOf(argument)) {
                                                System.out.println("LONG TYPE");
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
                String newCommand;
                if (commandProcessing.getProcessingCommand().contains("DELETE")) {
                    int deletePosition = commandProcessing.getProcessingCommand().indexOf("DELETE");
                    newCommand = commandProcessing.getProcessingCommand().substring(deletePosition);
                } else {
                    newCommand = "";
                }
                commandProcessing.updateCommand(newCommand);
                return finalResult;
            }

            private PropertyType getArgumentType(String argument) {
                return argument.contains("'") ? STRING : LONG;
            }

//            private void identityFunction(Result result) {
//
//            }
        };




        Predicate<String> identityFuncPredicate = x -> x.matches("id\\(([\\w+]+)\\) = (\\d+)");

        Transition identityFunctionTransition = new Transition(extractConditionsPart, endState, identityFuncPredicate, identityFuncAction);


        DfaAction conditionAction = new DfaAction() {

            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                System.out.println("Condition Action!");
                System.out.println(commandProcessing.getProcessingCommand());
                String regex = "([\\w]+).([\\w]+) ([=><]+) ([\\w']+) ?(AND|OR)? ?([\\w]+)?.?([\\w]+)? ?([=><]+)? ?([\\w']+)?.*";

                Map<Integer, ResultRow> matchedNodes = new HashMap<>();

                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(commandProcessing.getProcessingCommand());
                if (matcher.find()) {

                    System.out.println("GRoup: " + matcher.groupCount());

                    // if occurs only single exuation
                    if (matcher.groupCount() == 9) {
                        String variable = matcher.group(1);
                        String property = matcher.group(2);
                        String operator = matcher.group(3);
                        String value = matcher.group(4);

                        System.out.println(variable);
                        System.out.println(property);
                        System.out.println(operator);
                        System.out.println(value);

                        String logicalOperatorSymbol = matcher.group(5);
                        String variable2 = matcher.group(6);
                        String property2 = matcher.group(7);
                        String operator2 = matcher.group(8);
                        String value2 = matcher.group(9);

                        System.out.println(logicalOperatorSymbol);
                        System.out.println(variable2);
                        System.out.println(property2);
                        System.out.println(operator2);
                        System.out.println(value2);

                        List<Node> nodeList = nodesWithProperty(result, property);

                        Optional<Property> argumentB = transformToProperty(property, value);
                        Optional<Property> argumentC = transformToProperty(property2, value2);

                        int index = 0;
                        for (Node node : nodeList) {
                            Optional<Property> argumentA = node.getProperty(property);

                            ArithmeticOperator arithmeticOperator = getArithmeticOperator(operator);

                            Equation equation1 = new Equation(argumentA, arithmeticOperator, argumentB);

                            ArithmeticOperator arithmeticOperator2 = getArithmeticOperator(operator2);
                            LogicalOperator logicalOperator = getLogicalOperator(logicalOperatorSymbol);
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

                        String newCommand;
                        if (commandProcessing.getProcessingCommand().contains("DELETE")) {
                            int deletePosition = commandProcessing.getProcessingCommand().indexOf("DELETE");
                            newCommand = commandProcessing.getProcessingCommand().substring(deletePosition);
                        }
                        else if (commandProcessing.getProcessingCommand().contains("SET")) {
                            int deletePosition = commandProcessing.getProcessingCommand().indexOf("SET");
                            newCommand = commandProcessing.getProcessingCommand().substring(deletePosition);
                        }
                        else if (commandProcessing.getProcessingCommand().contains("REMOVE")) {
                            int deletePosition = commandProcessing.getProcessingCommand().indexOf("REMOVE");
                            newCommand = commandProcessing.getProcessingCommand().substring(deletePosition);
                        }
                        else {
                            newCommand = "";
                        }
                        commandProcessing.updateCommand(newCommand);
                    }
                }

                result.getResults().clear();
                result.getResults().putAll(matchedNodes);
                return result;
            }

            private Optional<Property> transformToProperty(String property, String value) {
                if (property == null || value == null) {
                    return Optional.empty();
                }

                PropertyType propertyType = getArgumentType(value);
                switch (propertyType) {
                    case LONG:
                        return Optional.of(new Property(property, propertyType, new Long(value)));

                    case STRING:
                        return Optional.of(new Property(property, propertyType, (String) value.substring(1, value.length()-1)));

                    default:
                        throw new IllegalArgumentException("type does not recognized!");
                }
            }

            private List<Node> nodesWithProperty(Result result, String property) {
                return result.getResults().entrySet().stream().map(entry ->  entry.getValue().getNode())
                        .filter(node -> node.getAllProperties().hasProperty(property))
                        .collect(Collectors.toList());
            }

            private ArithmeticOperator getArithmeticOperator(String operator) {
                return ArithmeticOperator.getOperator(operator);
            }

            private LogicalOperator getLogicalOperator(String operator) {
                return LogicalOperator.getOperator(operator);
            }

            private PropertyType getArgumentType(String argument) {
                return argument.contains("'") ? STRING : LONG;
            }

            class EquationResolver {
                private Equation firstEquation;
                private Equation secondEquation;
                private LogicalOperator logicalOperator;

                public EquationResolver(Equation firstEquation, LogicalOperator logicalOperator, Equation secondEquation) {
                    this.firstEquation = firstEquation;
                    this.secondEquation = secondEquation;
                    this.logicalOperator = logicalOperator;
                }

                public boolean validate() {

                    if (logicalOperator == null && !secondEquation.isFilled()) {
                        return firstEquation.isTrue();
                    } else {
                        if (logicalOperator == LogicalOperator.AND) {
                            return firstEquation.isTrue() && secondEquation.isTrue();
                        }

                        if (logicalOperator == LogicalOperator.OR) {
                            return firstEquation.isTrue() || secondEquation.isTrue();
                        }
                    }

                    return false;
                }
            }

            class Equation {
                private Optional<Property> argumentA;
                private Optional<Property> argumentB;
                private ArithmeticOperator arithmeticOperator;

                public Equation(Optional<Property> argumentA, ArithmeticOperator arithmeticOperator, Optional<Property> argumentB) {
                    this.argumentA = argumentA;
                    this.argumentB = argumentB;
                    this.arithmeticOperator = arithmeticOperator;
                }

                public boolean isFilled() {
                    return argumentA.isPresent() && argumentB.isPresent() && arithmeticOperator != null;
                }

                public boolean isTrue() {

                    argumentA.get().getType();

                    PropertyType type = argumentB.get().getType();

                    switch (type) {
                        case LONG:
                            return longValues((long) argumentA.get().getValue(), (long) argumentB.get().getValue(), arithmeticOperator);

                        case STRING:
                            System.out.println("STRING");
                            return textValues((String) argumentA.get().getValue(), (String) argumentB.get().getValue());

                        default:
                            throw new IllegalArgumentException("");
                    }
                }

                boolean longValues(long valueA, long valueB, ArithmeticOperator operator) {
                    switch (operator) {
                        case EQUALS:
                            return valueA == valueB;

                        case GRATER:
                            return valueA > valueB;

                        case GREATER_THAN_OR_EQUALS:
                            return valueA >= valueB;

                        case LOWER:
                            return valueA < valueB;

                        case LOWER_THAN_OR_EQUAL:
                            return valueA <= valueB;

                        default:
                            throw new IllegalArgumentException("Equation operator invalid!");
                    }
                }

                boolean textValues(String valueA, String valueB) {
                    return valueA.equals(valueB);
                }
            }

        };

//        Predicate<String> conditionFunctionPredicate = x -> x.matches("([\\w]+).([\\w]+) ([=>]+) ([\\w']+)");
        Predicate<String> conditionFunctionPredicate = x -> x.matches("([\\w]+).([\\w]+) ([=><]+) ([\\w']+) ?(AND|OR)? ?([\\w]+)?.?([\\w]+)? ?([=><]+)? ?([\\w']+)?.*");
//        Predicate<String> conditionFunctionPredicate = x -> x.matches("([\\w]+).([\\w]+) ([=><]+) ([\\w']+)( (AND|OR) ([\\w]+).([\\w]+) ([=><]+) ([\\w']+))?");
        Transition conditionFunctionTransition = new Transition(extractConditionsPart, endState, conditionFunctionPredicate, conditionAction);

        return endState;
    }

    private void conditionFunction() {

    }

    private void identityFunction() {

    }

}
