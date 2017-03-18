package org.hajecsdb.graphs.cypher.clauses;


import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.Query;
import org.hajecsdb.graphs.cypher.Result;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hajecsdb.graphs.cypher.CommandType.CREATE_NODE;

public class CreateNodeClauseBuilder extends ClauseBuilder {

    public CreateNodeClauseBuilder(Graph graph) {
        super(graph);
    }

    @Override
    public State buildClause(DFA dfa, State state) {
        List<State> stateList = new LinkedList<>();
        List<Transition> transitionList = new LinkedList<>();

        State verifyCreateClause = state;
        State extractNodePart = new State("extract node part!");
        State endState = new State("get processed Data!");

        Predicate<String> createClausePredicate = x -> x.startsWith("CREATE ");

        DfaAction extractNodePartAction = new DfaAction() {

            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                commandProcessing.recordProcessedPart("CREATE ", currentState);
                String commandToProcessed = commandProcessing.getProcessingCommand().substring(7);
                commandProcessing.updateCommand(commandToProcessed);
                return result;
            }
        };

        DfaAction extractNodeParamsAction = new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                commandProcessing.recordProcessedPart(commandProcessing.getProcessingCommand(), currentState);
//                String regex = "\\(([\\w]*): ([\\w]*)\\)";
                String regex = "\\(([\\w]*): ([\\w]+)( )?(\\{[\\w: ',.]*\\})?\\)";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(commandProcessing.getProcessingCommand());
                if (matcher.find()) {
                    System.out.println("#" + matcher.group() + "#");
                    String variableName = matcher.group(1);
//                    System.out.println("#" + variableName + "#");
                    Label label = new Label(matcher.group(2));
//                    System.out.println("#" + label + "#");
                    List<Property> parameters = new LinkedList<>();

                    if (matcher.groupCount() == 4 && matcher.group(4) != null) {
                        String paramContent = matcher.group(4);
                        String paramRegex = "([\\w]*): ([\\w'.]*)";
                        Pattern paramPattern = Pattern.compile(paramRegex);
                        Matcher paramsMatcher = paramPattern.matcher(paramContent);

                        while(paramsMatcher.find()) {
                            String variable = paramsMatcher.group(1);
//                            System.out.println("#" + variable + "#");
                            String value = paramsMatcher.group(2);
//                            System.out.println("#" + value + "#");
                            Property property = parameterExtractor.extract(variable, value);

                            parameters.add(property);

                        }
                        matcher.group(4);
                    }

                    Query query = new Query(commandProcessing.getProcessingCommand(), CREATE_NODE, variableName, label, parameters);
                    createNode(query, graph, result);
                    commandProcessing.getQueries().add(query);
                }

                commandProcessing.updateCommand("");
                return result;
            }

            void createNode(Query query, Graph graph,  Result result) {
                if (query.getParameters().isEmpty()) {
                    graph.createNode(query.getLabel());
                } else {
                    Properties properties = new Properties();
                    properties.addAll(query.getParameters());
                    graph.createNode(query.getLabel(), properties);
                }
                result.setCompleted(true);
            }

        };

        // \(([\w]*): ([\w]+)( )?({[\w: ',]*})?\)
        Predicate<String> nodeParamsPredicate = x -> x.matches("\\(([\\w]*): ([\\w]+)( )?(\\{[\\w: ',.]*\\})?\\)");

        Transition createClauseTransition = new Transition(verifyCreateClause, extractNodePart, createClausePredicate, extractNodePartAction);

        Transition extractParametersTransition = new Transition(extractNodePart, endState, nodeParamsPredicate, extractNodeParamsAction);

        stateList.addAll(Arrays.asList(verifyCreateClause, extractNodePart, endState));
        transitionList.addAll(Arrays.asList(createClauseTransition, extractParametersTransition));

//        dfa.setBeginState(verifyCreateClause);
//        dfa.addStates(stateList);
//        dfa.addTransitions(transitionList);
        return endState;
    }
}
