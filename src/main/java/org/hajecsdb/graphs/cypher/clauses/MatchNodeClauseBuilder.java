package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.Query;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hajecsdb.graphs.cypher.CommandType.MATCH_NODE;
import static org.hajecsdb.graphs.cypher.ContentType.NODE;

public class MatchNodeClauseBuilder extends ClauseBuilder {

    public MatchNodeClauseBuilder(Graph graph) {
        super(graph);
    }

    @Override
    public State buildClause(DFA dfa, State state) {
        List<State> stateList = new LinkedList<>();
        List<Transition> transitionList = new LinkedList<>();
        State verifyMatchClause = state;
        State extractNodePart = new State("[MATCH] extract node part!");
        State endState = new State("get processed Data!");


        Predicate<String> matchClausePredicate = x -> x.startsWith("MATCH ");

        DfaAction extractNodePartAction = new DfaAction() {

            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                commandProcessing.recordProcessedPart("MATCH ", currentState);
                String commandToProcessed = commandProcessing.getProcessingCommand().substring(6);
                commandProcessing.updateCommand(commandToProcessed);
                return result;
            }
        };

        Transition matchClauseTransition = new Transition(verifyMatchClause, extractNodePart, matchClausePredicate, extractNodePartAction);


        DfaAction extractNodeParamsAction = new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                commandProcessing.recordProcessedPart(commandProcessing.getProcessingCommand(), currentState);
                String regex = "\\(([\\w]+):? ?([\\w]*)\\)";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(commandProcessing.getProcessingCommand());
                if (matcher.find()) {
                    System.out.println("#" + matcher.group() + "#");
                    String variableName = matcher.group(1);
                    System.out.println("#" + variableName + "#");
                    Label label = new Label(matcher.group(2));
                    System.out.println("#" + label + "#");

                    Query query = new Query(commandProcessing.getProcessingCommand(), MATCH_NODE, variableName, label, null);
                    matchNode(query, graph, result);
                    commandProcessing.getQueries().add(query);
                }

                String str = commandProcessing.getProcessingCommand().substring(commandProcessing.getProcessingCommand().indexOf(")")+1, commandProcessing.getProcessingCommand().length());
                if (!str.isEmpty()) {
                    str = str.substring(1);
                }
                commandProcessing.updateCommand(str);
                return result;
            }

            void matchNode(Query query, Graph graph, Result result) {
                List<Node> filteredNodesByLabel = null;

                if (query.getLabel().getName().isEmpty()) {
                    filteredNodesByLabel = graph.getAllNodes().stream().collect(Collectors.toList());
                } else {
                    filteredNodesByLabel = graph.getAllNodes().stream()
                            .filter(node -> node.getLabel().equals(query.getLabel()))
                            .collect(Collectors.toList());
                }
                for (int i = 0; i < filteredNodesByLabel.size(); i++) {
                    ResultRow resultRow = new ResultRow();
                    resultRow.setContentType(NODE);
                    resultRow.setNode(filteredNodesByLabel.get(i));
                }

                List<ResultRow> resultRows = filteredNodesByLabel.stream().map(node -> {
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
        Predicate<String> nodeParamsPredicate = x -> x.matches("\\(([\\w]+):? ?([\\w]*)\\) ?.*");

        Transition extractParametersTransition = new Transition(extractNodePart, endState, nodeParamsPredicate, extractNodeParamsAction);


        stateList.addAll(Arrays.asList(verifyMatchClause, extractNodePart));
        transitionList.addAll(Arrays.asList(matchClauseTransition));
        dfa.setBeginState(verifyMatchClause);
        dfa.addStates(stateList);
        dfa.addTransitions(transitionList);
        return endState;
    }
}
