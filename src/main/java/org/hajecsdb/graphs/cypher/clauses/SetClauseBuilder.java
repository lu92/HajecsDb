package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

public class SetClauseBuilder extends ClauseBuilder {
    public SetClauseBuilder(Graph graph) {
        super(graph);
    }

    @Override
    public State buildClause(DFA dfa, State state) {
        State verifySetClause = state;
        State performSetPart = new State("[SET] extract condition!");
        State endState = new State("[SET] get processed Data!");

        Predicate<String> setClausePredicate = x -> x.startsWith("SET ");

        DfaAction extractNodePartAction = new DfaAction() {

            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                System.out.println("[SET] clause validate!");
                String updatedCommandToProceed = commandProcessing.getProcessingCommand().substring(4);
                commandProcessing.updateCommand(updatedCommandToProceed);
                return result;
            }
        };

        Transition matchClauseTransition = new Transition(verifySetClause, performSetPart, setClausePredicate, extractNodePartAction);

        DfaAction performCondition = new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                System.out.println("[SET] perform clause!");
                String regex = "([\\w]+).([\\w]+) = ([\\w']+)";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(commandProcessing.getProcessingCommand());
                if (matcher.find()) {
                    String variable = matcher.group(1);
                    String property = matcher.group(2);
                    String value = matcher.group(3);
                    Optional<Property> propertyOptional = transformToProperty(property, value);
                    setOrUpdate(result, propertyOptional.get());
                }
                commandProcessing.updateCommand("");
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

            private PropertyType getArgumentType(String argument) {
                return argument.contains("'") ? STRING : LONG;
            }
        };

        Predicate<String> setPredicate = x -> x.matches("([\\w]+).([\\w]+) = ([\\w']+)");

        State performConditionState = new State("[SET] performing condition!");

        Transition transition = new Transition(performSetPart, performConditionState, setPredicate, performCondition);
        return endState;
    }
}
