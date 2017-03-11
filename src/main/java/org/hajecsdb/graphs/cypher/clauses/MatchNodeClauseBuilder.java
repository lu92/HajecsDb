package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.Query;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hajecsdb.graphs.cypher.CommandType.MATCH_NODE;

public class MatchNodeClauseBuilder extends ClauseBuilder {

    @Override
    public void buildClause(DFA dfa) {
        List<State> stateList = new LinkedList<>();
        List<Transition> transitionList = new LinkedList<>();
        State verifyMatchClause = dfa.getBeginState();
        State extractNodePart = new State("[MATCH] extract node part!");
        State endState = new State("get processed Data!");


        Predicate<String> matchClausePredicate = x -> x.startsWith("MATCH ");

        DfaAction extractNodePartAction = new DfaAction() {

            @Override
            public void perform(State currentState, CommandProcessing commandProcessing) {
                commandProcessing.recordProcessedPart("MATCH ", currentState);
                String commandToProcessed = commandProcessing.getProcessingCommand().substring(6);
                commandProcessing.updateCommand(commandToProcessed);
            }
        };

        Transition matchClauseTransition = new Transition(verifyMatchClause, extractNodePart, matchClausePredicate, extractNodePartAction);


        DfaAction extractNodeParamsAction = new DfaAction() {
            @Override
            public void perform(State currentState, CommandProcessing commandProcessing) {
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
                    commandProcessing.getQueries().add(query);
                }

                commandProcessing.updateCommand("");
            }

        };
        Predicate<String> nodeParamsPredicate = x -> x.matches("\\(([\\w]+):? ?([\\w]*)\\)");

        Transition extractParametersTransition = new Transition(extractNodePart, endState, nodeParamsPredicate, extractNodeParamsAction);


        stateList.addAll(Arrays.asList(verifyMatchClause, extractNodePart));
        transitionList.addAll(Arrays.asList(matchClauseTransition));
        dfa.setBeginState(verifyMatchClause);
        dfa.addStates(stateList);
        dfa.addTransitions(transitionList);
    }
}
