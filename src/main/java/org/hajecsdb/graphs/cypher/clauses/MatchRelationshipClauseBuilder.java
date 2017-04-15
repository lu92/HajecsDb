package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.Direction;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.clauses.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.clauses.DFA.State;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor.ParameterExtractor;
import org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor.SubQueryData;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum.MATCH_RELATIONSHIP;

public class MatchRelationshipClauseBuilder extends ClauseBuilder {

    public MatchRelationshipClauseBuilder() {
        super(MATCH_RELATIONSHIP);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, CommandProcessing commandProcessing) {

                SubQueryData subQueryData = parameterExtractor.fetchData(commandProcessing.getClauseInvocationStack().peek().getSubQuery());

                List<Node> nodes = null;
                if (subQueryData.getLeftNode().getLabel().isPresent()) {
                    nodes = graph.getAllNodes().stream()
                            .filter(node -> node.getLabel().equals(subQueryData.getLeftNode().getLabel().get()))
                            .collect(Collectors.toList());
                } else {
                    nodes = new ArrayList<>();
                    nodes.addAll(graph.getAllNodes());
                }


                if (!subQueryData.getLeftNode().getParameters().isEmpty()) {

                    // filter by left node's parameters
                    nodes = nodes.stream()
                            .filter(node -> node.getAllProperties().getAllProperties().containsAll(subQueryData.getLeftNode().getParameters()))
                            .collect(Collectors.toList());
                }

                if (!subQueryData.getRightNode().getParameters().isEmpty()) {

                    // filter by right node's parameters
                    nodes = nodes.stream()
                            .filter(node -> node.getAllProperties().getAllProperties().containsAll(subQueryData.getRightNode().getParameters()))
                            .collect(Collectors.toList());
                }

                List<Node> neighbours = getNeighbours(nodes, subQueryData.getRelationship().getDirection());

                IntStream.range(0, neighbours.size()).forEach(index -> {
                    ResultRow resultRow = new ResultRow();
                    resultRow.setContentType(ContentType.NODE);
                    resultRow.setNode(neighbours.get(index));
                    result.getResults().put(index, resultRow);
                });

                result.setCompleted(true);
                return result;
            }

            private List<Node> getNeighbours(List<Node> nodes, Direction direction) {
                switch (direction) {
                    case BOTH:
//                        System.out.println("BOTH");
                        return nodes.stream()
                                .map(node -> node.getRelationships())
                                .flatMap(relationships -> relationships.stream())
                                .map(Relationship::getEndNode)
                                .distinct()
                                .collect(Collectors.toList());

                    case OUTGOING:
//                        System.out.println("OUTGOING");
                        return nodes.stream()
                                .map(node -> node.getRelationships())
                                .flatMap(relationships -> relationships.stream())
                                .filter(relationship -> relationship.getDirection() == Direction.OUTGOING)
                                .map(Relationship::getEndNode)
                                .distinct()
                                .collect(Collectors.toList());

                    case INCOMING:
//                        System.out.println("INCOMING");
                        return nodes.stream()
                                .map(node -> node.getRelationships())
                                .flatMap(relationships -> relationships.stream())
                                .filter(relationship -> relationship.getDirection() == Direction.INCOMING)
                                .map(Relationship::getEndNode)
                                .distinct()
                                .collect(Collectors.toList());


                    default:
                        return new ArrayList<>();
                }
            }
        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        //      \(([\w]+):?([\w]+)?(\{[\w:' }]+)?\)(--|-\[([\w]+):?([\w]+)?\]->|<-\[([\w]+):?([\w]+)?\]-)\(([\w]+):?([\w]+)?(\{[\w:' }]+)?\)
        return "\\(([\\w]+):?([\\w]+)?(\\{[\\w:' }]+)?\\)(--|-\\[([\\w]+):?([\\w]+)?\\]->|<-\\[([\\w]+):?([\\w]+)?\\]-)\\(([\\w]+):?([\\w]+)?(\\{[\\w:' }]+)?\\)";
    }
}
