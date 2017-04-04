package org.hajecsdb.graphs;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.storage.GraphStorage;
import org.hajecsdb.graphs.transactions.*;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

public class GraphService implements Graph, Transactional {
    private Graph graph;
    private TransactionManager transactionManager;
    private Transaction transaction;
    private GraphStorage graphStorage;

    public GraphService(String pathDir, String graphName, GraphStorage graphStorage) throws IOException {
        this.graphStorage = graphStorage;
        graph = graphStorage.loadGraph(pathDir + "/" + graphName);
        transactionManager = new TransactionManager();
    }

    @Override
    public void beginTransaction() {
        transaction = transactionManager.createTransaction();
    }

    @Override
    public Transaction getTransaction() throws TransactionException {
        if (transaction == null)
            throw new TransactionException("The transaction has not been started");
        return transaction;
    }

    @Override
    public String getPathDir() {
        return graph.getPathDir();
    }

    @Override
    public String getGraphName() {
        return graph.getGraphName();
    }

    @Override
    public String getFilename() {
        return graph.getFilename();
    }

    @Override
    public Properties getProperties() {
        return graph.getProperties();
    }

    @Override
    public Node createNode() {
        Node node = graph.createNode();
        transaction.getScope().add(node, OperationType.CREATE);
        return node;
    }

    @Override
    public Node createNode(Properties properties) {
        Node node = graph.createNode(properties);
        transaction.getScope().add(node, OperationType.CREATE);
        return node;
    }

    @Override
    public Node createNode(Label label) {
        Node node = graph.createNode(label);
        transaction.getScope().add(node, OperationType.CREATE);
        return node;
    }

    @Override
    public Node createNode(Label label, Properties properties) {
        Node node = graph.createNode(label, properties);
        transaction.getScope().add(node, OperationType.CREATE);
        return node;
    }

    @Override
    public Node addProperties(long nodeId, Properties properties) {
        Node node = graph.addProperties(nodeId, properties);
        transaction.getScope().add(node, OperationType.UPDATE);
        return node;
    }

    @Override
    public Optional<Node> getNodeById(long id) {
        return null;
    }

    @Override
    public Set<Node> getAllNodes() {
        return null;
    }

    @Override
    public Node deleteNode(long id) {
        return null;
    }

    @Override
    public Relationship findRelationship(long beginNodeId, long endNodeId, Label label) {
        return null;
    }

    @Override
    public Optional<Relationship> getRelationshipById(long id) {
        return null;
    }

    @Override
    public Relationship deleteRelationship(long id) {
        return null;
    }

    @Override
    public Set<Relationship> getAllRelationships() {
        return null;
    }

    @Override
    public Iterable<Label> getAllLabels() {
        return null;
    }

    @Override
    public Relationship createRelationship(Node beginNode, Node endNode, Label label) {
        Relationship relationship = graph.createRelationship(beginNode, endNode, label);
        transaction.getScope().add(relationship, OperationType.CREATE);
        return relationship;
    }

    @Override
    public Relationship createRelationship(Node beginNode, String type, Node endNode) {
        Relationship relationship = graph.createRelationship(beginNode, type, endNode);
        transaction.getScope().add(relationship, OperationType.CREATE);
        return relationship;
    }
}
