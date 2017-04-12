package org.hajecsdb.graphs.cypher.DFA;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ParameterExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public interface DfaAction {
    Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing);

    ParameterExtractor parameterExtractor = new ParameterExtractor();

    default List<Property> extractParameters(String parametersBody) {
        List<Property> parameters = new ArrayList<>();
        if (isNotEmpty(parametersBody)) {
            String paramRegex = "([\\w]*):([\\w'.]*)";
            Pattern paramPattern = Pattern.compile(paramRegex);
            Matcher paramsMatcher = paramPattern.matcher(parametersBody);

            while (paramsMatcher.find()) {
                String variable = paramsMatcher.group(1);
                String value = paramsMatcher.group(2);
                Property property = parameterExtractor.extract(variable, value);
                parameters.add(property);
            }
        }
        return parameters;
    }
}
