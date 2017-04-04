package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.DFA.State;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateRelationshipClauseBuilder extends ClauseBuilder {

    public CreateRelationshipClauseBuilder(Graph graph) {
        super(ClauseEnum.CREATE_RELATIONSHIP, graph);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    RelationshipData relationshipData = getData(matcher);
                    Result resultOfLeftNodes = commandProcessing.getQueryContext().get(relationshipData.leftNodesVariableName);
                    Result resultOfRightNodes = commandProcessing.getQueryContext().get(relationshipData.rightNodesVariableName);
                    if (resultOfLeftNodes.hasContent() && resultOfRightNodes.hasContent()) {
                        for (Map.Entry<Integer, ResultRow> entry : resultOfLeftNodes.getResults().entrySet()) {
                            Node leftNode = entry.getValue().getNode();
                            for (Map.Entry<Integer, ResultRow> entry2 : resultOfRightNodes.getResults().entrySet()) {
                                Node rightNode = entry2.getValue().getNode();
                                graph.createRelationship(leftNode, rightNode, new Label(relationshipData.relatonshipLabel.getName()));
                            }
                        }
                    }
                }
                return result;
            }

            class RelationshipData {
                String leftNodesVariableName;
                Label leftNodesLabel;
                String relationshipName;
                Label relatonshipLabel;
                String rightNodesVariableName;
                Label rightNodesLabel;
            }

            public RelationshipData getData(Matcher matcher) {
                RelationshipData relationshipData = new RelationshipData();
                relationshipData.leftNodesVariableName = matcher.group(1);
                relationshipData.leftNodesLabel = new Label(matcher.group(2));
                relationshipData.relationshipName = matcher.group(4);
                relationshipData.relatonshipLabel = new Label(matcher.group(5));
                relationshipData.rightNodesVariableName = matcher.group(6);
                relationshipData.rightNodesLabel = new Label(matcher.group(7));
                return relationshipData;
            }
        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "\\(([\\w]+):?([\\w]+)?\\)<?-\\[(([\\w]+)?:?([\\w]+)?)\\]->?\\(([\\w]+):?([\\w]+)?\\)";
    }
}
