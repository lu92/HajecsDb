package org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor;

import org.hajecsdb.graphs.core.Direction;
import org.hajecsdb.graphs.core.Label;

import java.util.Optional;

public final class RelationshipData {
    private Optional<String> variable;
    private Optional<Label> label;
    private Direction direction;

    public RelationshipData(Optional<String> variable, Optional<Label> label, Direction direction) {
        this.variable = variable;
        this.label = label;
        this.direction = direction;
    }

    public Optional<String> getVariable() {
        return variable;
    }

    public Optional<Label> getLabel() {
        return label;
    }

    public Direction getDirection() {
        return direction;
    }
}