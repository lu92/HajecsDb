package org.hajecsdb.graphs.cypher.clauses;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DfaAction;
import org.hajecsdb.graphs.cypher.DFA.State;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.hajecsdb.graphs.cypher.clauses.ClauseEnum.MATCH_RELATIONSHIP;

public class MatchRelationshipClauseBuilder extends ClauseBuilder {

    public MatchRelationshipClauseBuilder() {
        super(MATCH_RELATIONSHIP);
    }

    @Override
    public DfaAction clauseAction() {
        return new DfaAction() {
            @Override
            public Result perform(Graph graph, Result result, State currentState, CommandProcessing commandProcessing) {
                SubQueryData subQueryData = fetchData(commandProcessing.getClauseInvocationStack().peek().getSubQuery());

                List<Node> nodes = null;
                if (subQueryData.leftNode.getLabel().isPresent()) {
                    nodes = graph.getAllNodes().stream()
                            .filter(node -> node.getLabel().equals(subQueryData.leftNode.getLabel().get()))
                            .collect(Collectors.toList());
                } else {
                    nodes = new ArrayList<>();
                    nodes.addAll(graph.getAllNodes());
                }


                if (!subQueryData.leftNode.getParameters().isEmpty()) {

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

                List<Node> neighbours = getNeighbours(nodes, subQueryData.getRelationship().direction);

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
                        System.out.println("BOTH");
                        return nodes.stream()
                                .map(node -> node.getRelationships())
                                .flatMap(relationships -> relationships.stream())
                                .map(Relationship::getEndNode)
                                .distinct()
                                .collect(Collectors.toList());

                    case OUTGOING:
                        System.out.println("OUTGOING");
                        return nodes.stream()
                                .map(node -> node.getRelationships())
                                .flatMap(relationships -> relationships.stream())
                                .filter(relationship -> relationship.getDirection() == Direction.OUTGOING)
                                .map(Relationship::getEndNode)
                                .distinct()
                                .collect(Collectors.toList());

                    case INCOMING:
                        System.out.println("INCOMING");
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

            private SubQueryData fetchData(String text) {
                String regex = "(\\([\\w :'{}]+\\))([-><:_\\[\\]\\w]*)?(\\([\\w :'{}]+\\))";
                Matcher mather = Pattern.compile(regex).matcher(text);

                if (mather.find()) {

                    String leftNodePart = mather.group(1);
                    String relationshipPart = mather.group(2);
                    String rightNodePart = mather.group(3);

                    SubQueryData subQueryData = new SubQueryData(
                            fetchNode(leftNodePart),
                            fetchRelationship(relationshipPart),
                            fetchNode(rightNodePart));

                    return subQueryData;
                }
                throw new IllegalArgumentException("Cannot read relationship!");
            }

            private RelationshipData fetchRelationship(String relationshipContent) {
                return Arrays.asList(notDirectedRelationship(relationshipContent),
                        incomingRelationship(relationshipContent),
                        outgoingRelationship(relationshipContent)).stream()
                        .filter(relationshipData -> relationshipData.isPresent()).findFirst().get()
                        .orElseThrow(() -> new IllegalArgumentException("INTERNAL ERROR!"));
            }

            private Optional<RelationshipData> notDirectedRelationship(String relationshipContent) {
                if (relationshipContent.equals("--")) {
                    return Optional.of(new RelationshipData(null, null, Direction.BOTH));
                }
                return Optional.empty();
            }

            private Optional<RelationshipData> incomingRelationship(String relationshipContent) {
                String relationshipRegex = "(<-\\[([\\w]+):?([\\w]+)?\\]-)";
                Matcher matcher = Pattern.compile(relationshipRegex).matcher(relationshipContent);
                if (matcher.find()) {
                    Optional<String> variable = Optional.ofNullable(matcher.group(2));
                    Optional<Label> label = isNotEmpty(matcher.group(3)) ? Optional.of(new Label(matcher.group(3))) : Optional.empty();
                    Direction direction = Direction.INCOMING;
                    return Optional.of(new RelationshipData(variable, label, direction));
                }
                return Optional.empty();
            }

            private Optional<RelationshipData> outgoingRelationship(String relationshipContent) {
                String relationshipRegex = "(-\\[([\\w]+):?([\\w]+)?\\]->)";
                Matcher matcher = Pattern.compile(relationshipRegex).matcher(relationshipContent);
                if (matcher.find()) {
                    Optional<String> variable = Optional.ofNullable(matcher.group(2));
                    Optional<Label> label = isNotEmpty(matcher.group(3)) ? Optional.of(new Label(matcher.group(3))) : Optional.empty();
                    Direction direction = Direction.OUTGOING;
                    return Optional.of(new RelationshipData(variable, label, direction));
                }
                return Optional.empty();
            }

            private NodeData fetchNode(String nodeContent) {
                String nodeRegex = "\\(([\\w]+):?([\\w]+)?(\\{[\\w:' }]+)?\\)";
                Matcher matcher = Pattern.compile(nodeRegex).matcher(nodeContent);


                if (matcher.find()) {
                    Optional<String> variableName = Optional.ofNullable(matcher.group(1));
                    Optional<Label> label = isNotEmpty(matcher.group(2)) ? Optional.of(new Label(matcher.group(2))) : Optional.empty();
                    List<Property> parameters = new LinkedList<>();

                    if (isNotEmpty(matcher.group(3))) {
                        String paramContent = matcher.group(3);
                        String paramRegex = "([\\w]*):([\\w'.]*)";
                        Pattern paramPattern = Pattern.compile(paramRegex);
                        Matcher paramsMatcher = paramPattern.matcher(paramContent);

                        while (paramsMatcher.find()) {
                            String variable = paramsMatcher.group(1);
                            String value = paramsMatcher.group(2);
                            Property property = parameterExtractor.extract(variable, value);
                            parameters.add(property);
                        }
                    }
                    return new NodeData(variableName, label, parameters);
                }
                throw new IllegalArgumentException("");
            }

            final class SubQueryData {
                private NodeData leftNode;
                private RelationshipData relationship;
                private NodeData rightNode;

                public SubQueryData(NodeData leftNode, RelationshipData relationship, NodeData rightNode) {
                    this.leftNode = leftNode;
                    this.relationship = relationship;
                    this.rightNode = rightNode;
                }

                public NodeData getLeftNode() {
                    return leftNode;
                }

                public RelationshipData getRelationship() {
                    return relationship;
                }

                public NodeData getRightNode() {
                    return rightNode;
                }
            }

            final class NodeData {
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

            final class RelationshipData {
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
        };
    }

    @Override
    public String getExpressionOfClauseRegex() {
        //      \(([\w]+):?([\w]+)?(\{[\w:' }]+)?\)(--|-\[([\w]+):?([\w]+)?\]->|<-\[([\w]+):?([\w]+)?\]-)\(([\w]+):?([\w]+)?(\{[\w:' }]+)?\)
        return "\\(([\\w]+):?([\\w]+)?(\\{[\\w:' }]+)?\\)(--|-\\[([\\w]+):?([\\w]+)?\\]->|<-\\[([\\w]+):?([\\w]+)?\\]-)\\(([\\w]+):?([\\w]+)?(\\{[\\w:' }]+)?\\)";
    }
}
