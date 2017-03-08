package org.hajecsdb.graphs.cypher;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Property;

import java.util.LinkedList;
import java.util.List;

public class Query {
    private String partOfQuery;
    private CommandType commandType;
    private String variableName;
    private Label label;
    private List<Property> parameters;

    public Query(String partOfQuery, CommandType commandType, String variableName, Label label, List<Property> parameters) {
        this.partOfQuery = partOfQuery;
        this.commandType = commandType;
        this.variableName = variableName;
        this.label = label;
        this.parameters = parameters;
    }

    public String getPartOfQuery() {
        return partOfQuery;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    public Label getLabel() {
        return label;
    }

    public List<Property> getParameters() {
        if (parameters == null) {
            parameters = new LinkedList<>();
        }
        return parameters;
    }

    public String getVariableName() {
        return variableName;
    }
}
