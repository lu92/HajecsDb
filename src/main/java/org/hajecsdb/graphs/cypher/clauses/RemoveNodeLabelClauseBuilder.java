package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hajecsdb.graphs.core.PropertyType.STRING;

public class RemoveNodeLabelClauseBuilder extends ClauseBuilder {

    public RemoveNodeLabelClauseBuilder(Graph graph) {
        super(graph);
    }

    @Override
    public State buildClause(DFA dfa, State state) {
        State verifyRemoveClause = state;
        State performRemovePart = new State("[REMOVE] extract condition!");
        State endState = new State("[REMOVE] get processed Data!");

        Predicate<String> removeClausePredicate = x -> x.startsWith("REMOVE ");

        DfaAction extractNodePartAction = new DfaAction() {

            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                System.out.println("[REMOVE] clause validate!");
                String updatedCommandToProceed = commandProcessing.getProcessingCommand().substring(7);
                commandProcessing.updateCommand(updatedCommandToProceed);
                return result;
            }
        };

        Transition matchClauseTransition = new Transition(verifyRemoveClause, performRemovePart, removeClausePredicate, extractNodePartAction);


        DfaAction removeLabelAction = new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                System.out.println("[REMOVE] perform clause!");
                String regex = "([\\w]+)(:|.)([\\w]+)";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(commandProcessing.getProcessingCommand());
                if (matcher.find()) {
                    System.out.println("REMOVE]" + matcher.group(1) + " : " + matcher.group(3));
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

        Predicate<String> removeLabelPredicate = x -> x.matches("([\\w]+)(:|.)([\\w]+)");

        State removeLabelState = new State("[REMOVE] performing condition!");

        Transition transition = new Transition(performRemovePart, removeLabelState, removeLabelPredicate, removeLabelAction);

        return endState;
    }
}
