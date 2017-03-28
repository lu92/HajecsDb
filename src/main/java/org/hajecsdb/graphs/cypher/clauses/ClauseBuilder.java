package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.clauses.helpers.ParameterExtractor;

import java.util.Optional;
import java.util.function.Predicate;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

public abstract class ClauseBuilder {
    protected Graph graph;
    protected ClauseEnum clauseEnum;
    protected ParameterExtractor parameterExtractor;

    public ClauseBuilder(ClauseEnum clauseEnum, Graph graph) {
        this.graph = graph;
        this.clauseEnum = clauseEnum;
        this.parameterExtractor = new ParameterExtractor();
    }

    public abstract DfaAction clauseAction();
    public abstract String getExpressionOfClauseRegex();

    public State buildClause(DFA dfa, State state) {
        State clauseState = state;
        State actionState = new State(clauseEnum, "[" + clauseEnum + "] action state!");
        new Transition(clauseState, actionState, validateClause(), clauseAction());
        return actionState;
    }

    protected Predicate<ClauseInvocation> validateClause() {
        return clauseInvocation -> {
            boolean clauseNameValidation = getClauseNamePredicate().test(clauseInvocation.getClause().name());
            boolean subQueryValidation = getExpressionOfClausePredicate().test(clauseInvocation.getSubQuery());
            return clauseNameValidation && subQueryValidation;
        };
    }

    protected Predicate<String> getClauseNamePredicate() {
        return x -> x.equals(clauseEnum.name());
    }
    protected Predicate<String> getExpressionOfClausePredicate() {
        return x -> x.matches(getExpressionOfClauseRegex());
    }

    protected Optional<Property> transformToProperty(String property, String value) {
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

    protected PropertyType getArgumentType(String argument) {
        return argument.contains("'") ? STRING : LONG;
    }
}
