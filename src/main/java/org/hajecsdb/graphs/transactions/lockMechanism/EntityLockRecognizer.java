package org.hajecsdb.graphs.transactions.lockMechanism;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClausesSeparator;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor.ParameterExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EntityLockRecognizer {
    // okresla co ma byc zablokowane na podstawie komendy

    private ParameterExtractor parameterExtractor = new ParameterExtractor();
    private ClausesSeparator clausesSeparator = new ClausesSeparator();


    public List<Entity> determineEntities(Graph graph, String cypherQuery) {

        List<Entity> entitiesRequiredToBlock = new ArrayList<>();

        Stack<ClauseInvocation> clauseInvocations = clausesSeparator.splitByClauses(cypherQuery);
        ClauseInvocation clauseInvocation = clauseInvocations.pop();

        if (clauseInvocation.getClause() == ClauseEnum.MATCH_NODE) {
            Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
            Matcher matcher = pattern.matcher(clauseInvocation.getSubQuery());

            if (matcher.find()) {
                String variableName = matcher.group(1);
                Label label = new Label(matcher.group(2));
                String parametersBody = matcher.group(3);
                List<Property> parameters = parameterExtractor.extractParameters(parametersBody);

                return graph.getAllNodes().stream()
                        .filter(node -> node.getLabel().equals(label))
                        .collect(Collectors.toList());
            }
        }
        return entitiesRequiredToBlock;
    }

    String getExpressionOfClauseRegex() {
        return "\\(([\\w]+):?([\\w]*)(\\{[\\w:' ,]+\\})?\\)";
    }


}
