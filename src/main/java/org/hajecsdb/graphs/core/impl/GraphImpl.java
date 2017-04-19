package org.hajecsdb.graphs.core.impl;

import org.apache.commons.lang3.StringUtils;
import org.hajecsdb.graphs.IdGenerator;
import org.hajecsdb.graphs.core.*;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;


public class GraphImpl implements Graph {

    private IdGenerator idGenerator = new IdGenerator();
    private Set<Node> nodes = new HashSet<>();
    private Set<Relationship> relationships = new HashSet<>();

    private Properties properties = new Properties();

    public GraphImpl(String pathDir, String graphName) {
        if (StringUtils.isEmpty(pathDir) || StringUtils.isEmpty(graphName))
            throw new NullPointerException("pathDir and graphName can't be empty or null");
        properties.add(new Property("pathDir", STRING, pathDir));
        properties.add(new Property("graphName", STRING, graphName));
        properties.add(new Property("creationDateTime", STRING, LocalDateTime.now().toString()));
        properties.add(new Property("lastGeneratedId",LONG, idGenerator.getLastId()));
    }

    @Override
    public String getPathDir() {
        return (String) properties.getProperty("pathDir").get().getValue();
    }

    @Override
    public String getGraphName() {
        return (String) properties.getProperty("graphName").get().getValue();
    }

    @Override
    public String getFilename() {
        return getPathDir() + "/" + getGraphName() + ".json";
    }

    @Override
    public Properties getProperties() {
        Properties currentProperties = new Properties();
        currentProperties.addAll(properties);
        currentProperties.add(new Property("numberOfNodes", LONG, (long) getAllNodes().size()));
        currentProperties.add(new Property("numberOfRelationships", LONG, (long) getAllRelationships().size()));
        return currentProperties;

    }

    @Override
    public Node createNode() {
        Node node = new NodeImpl(idGenerator.generateId());
        nodes.add(node);
        return node;
    }

    @Override
    public Node createNode(Properties properties) {
        Node node = new NodeImpl(idGenerator.generateId());
        node.setProperties(properties);
        nodes.add(node);
        return node;
    }

    @Override
    public Node createNode(Label label) {
        Node node = new NodeImpl(idGenerator.generateId());
        node.setLabel(label);
        nodes.add(node);
        return node;
    }

    @Override
    public Node createNode(Label label, Properties properties) {
        Node node = new NodeImpl(idGenerator.generateId());
        node.setLabel(label);
        node.setProperties(properties);
        nodes.add(node);
        return node;
    }

    @Override
    public Node addProperties(long nodeId, Properties properties) {
        return null;
    }

    @Override
    public Optional<Node> getNodeById(long id) {
        return nodes.stream().filter(node -> node.getId() == id).findFirst();
    }

    @Override
    public Set<Node> getAllNodes() {
        return nodes;
    }

    @Override
    public Node deleteNode(long id) {
        Optional<Node> nodeById = getNodeById(id);

        if (nodeById.isPresent()) {
            if (!nodeById.get().hasRelationship()) {
                nodes.remove(nodeById.get());
                return nodeById.get();
            } else
                throw new IllegalArgumentException("Node has relationship!");
        } else
            throw new IllegalArgumentException("Node does not exist!");
    }

    @Override
    public Relationship findRelationship(long beginNodeId, long endNodeId, Label label) {
        return findRelationship(beginNodeId, endNodeId, Direction.OUTGOING, label);
    }

    @Override
    public Relationship findRelationship(long beginNodeId, long endNodeId, Direction direction, Label label) {
        Optional<Node> beginNodeById = getNodeById(beginNodeId);
        Optional<Node> secondNodeById = getNodeById(endNodeId);

        Predicate<Relationship> find = relationship -> relationship.getStartNode().getId() == beginNodeId
                && relationship.getEndNode().getId() == endNodeId && relationship.getLabel().equals(label)
                && relationship.getDirection() == direction;

        if (beginNodeById.isPresent() && secondNodeById.isPresent()) {

            Optional<Relationship> relationship =
                    beginNodeById.get().getRelationships().stream().filter(find).findFirst();

            if (relationship.isPresent())
                return relationship.get();
            else
                throw new IllegalArgumentException("Relationship does not exist!");

        } else
            throw new IllegalArgumentException("One or both of nodes don't exist!");
    }

    @Override
    public Optional<Relationship> getRelationshipById(long id) {
        return this.relationships.stream()
                .filter(relationship -> relationship.getId() == id)
                .findFirst();
    }

    @Override
    public Relationship deleteRelationship(long id) {
        Optional<Relationship> relationshipById = getRelationshipById(id);
        if (!relationshipById.isPresent()) {
            throw new IllegalArgumentException("Relationship does not exist!");
        }

        Relationship relationship = relationshipById.get();
        Node startNode = relationship.getStartNode();
        startNode.getRelationships().remove(relationship);
        Node endNode = relationship.getEndNode();
        endNode.getRelationships().remove(relationship.reverse());
        relationships.remove(relationship);
        relationships.remove(relationship.reverse());
        return relationship;
    }

    @Override
    public Set<Relationship> getAllRelationships() {
        return relationships;
    }

    @Override
    public Iterable<Label> getAllLabels() {
        Set<Label> allRelationshipTypes = new HashSet<>();
        for (Node node : getAllNodes()) {
            for (Relationship relationship : node.getRelationships()) {
                allRelationshipTypes.add(relationship.getLabel());
            }
        }
        return allRelationshipTypes;
    }

    @Override
    public Relationship createRelationship(Node beginNode, Node endNode, Label label) {
        if (beginNode == null || endNode == null) {
            throw new IllegalArgumentException("One or both nodes don't exist!");
        }

        // check node's relationships before addition
        if (label == null || StringUtils.isEmpty(label.getName())) {
            throw new IllegalArgumentException("Relationships type is null or empty!");
        }

        Relationship relationship = new RelationshipImpl(idGenerator.generateId(), beginNode, endNode, Direction.OUTGOING, label);
        if (this.relationships.contains(relationship))
            throw new IllegalArgumentException("Relationships already exists!");

        beginNode.addRelationShip(relationship);
        Relationship oppositeRelationship = new RelationshipImpl(idGenerator.generateId(), endNode, beginNode, Direction.INCOMING, label);
        endNode.addRelationShip(oppositeRelationship);
        this.relationships.add(relationship);
        this.relationships.add(oppositeRelationship);
        return relationship;
    }

    @Override
    public Relationship createRelationship(Node beginNode, String type, Node endNode) {
        return createRelationship(beginNode, endNode, new Label(type));
    }

}
