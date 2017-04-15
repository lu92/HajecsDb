package org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Property;

import java.util.List;
import java.util.Optional;

public class NodeData {
    private Optional<String> variable;
    private Optional<Label> label;
    private List<Property> parameters;

    public NodeData(Optional<String> variable, Optional<Label> label, List<Property> parameters) {
        this.variable = variable;
        this.label = label;
        this.parameters = parameters;
    }

    public Optional<String> getVariable() {
        return variable;
    }

    public Optional<Label> getLabel() {
        return label;
    }

    public List<Property> getParameters() {
        return parameters;
    }
}
