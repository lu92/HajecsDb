package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.clauses.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor.SubQueryData;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateRelationshipClauseBuilder extends ClauseBuilder {

    public CreateRelationshipClauseBuilder() {
        super(ClauseEnum.CREATE_RELATIONSHIP);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, CommandProcessing commandProcessing) {
                Pattern pattern = Pattern.compile(getExpressionOfClauseRegex());
                Matcher matcher = pattern.matcher(commandProcessing.getClauseInvocationStack().peek().getSubQuery());
                if (matcher.find()) {
                    SubQueryData subQueryData = parameterExtractor.fetchData(matcher.group());
                    Result resultOfLeftNodes = commandProcessing.getQueryContext().get(subQueryData.getLeftNode().getVariable().get());
                    Result resultOfRightNodes = commandProcessing.getQueryContext().get(subQueryData.getRightNode().getVariable().get());
                    if (resultOfLeftNodes.hasContent() && resultOfRightNodes.hasContent()) {
                        result.getResults().clear();
                        for (Map.Entry<Integer, ResultRow> entry : resultOfLeftNodes.getResults().entrySet()) {
                            Node leftNode = entry.getValue().getNode();
                            for (Map.Entry<Integer, ResultRow> entry2 : resultOfRightNodes.getResults().entrySet()) {
                                Node rightNode = entry2.getValue().getNode();
                                Relationship relationship = graph.createRelationship(leftNode, rightNode, subQueryData.getRelationship().getLabel().get());
                                int index = result.getResults().size()+1;
                                ResultRow resultRow = new ResultRow();
                                result.setCompleted(true);
                                resultRow.setContentType(ContentType.RELATIONSHIP);
                                resultRow.setRelationship(relationship);
                                result.getResults().put(index, resultRow);
                                commandProcessing.getQueryContext().insert(subQueryData.getRelationship().getVariable().get(), result.copy());
                            }
                        }
                    }
                }
                return result;
            }
        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        return "\\(([\\w]+):?([\\w]+)?\\)<?-\\[(([\\w]+)?:?([\\w]+)?)\\]->?\\(([\\w]+):?([\\w]+)?\\)";
    }
}
