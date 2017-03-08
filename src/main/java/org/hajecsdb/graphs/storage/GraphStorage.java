package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.NotFoundException;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.storage.entities.BinaryNode;
import org.hajecsdb.graphs.storage.entities.BinaryRelationship;

import java.io.IOException;
import java.util.Optional;

public interface GraphStorage {
    void saveGraph(Graph graph) throws IOException;
    Graph loadGraph(String filename) throws IOException;
    BinaryNode createNode(Node node) throws IOException;
    Optional<Node> readNode(long id) throws IOException;
    void updateNode(Node node) throws IOException, NotFoundException;
    void deleteNode(long id) throws IOException, NotFoundException;
    BinaryRelationship createRelationship(Relationship relationship) throws IOException;
    Optional<Relationship> readRelationship(long id) throws IOException;
    void updateRelationship(Relationship relationship) throws IOException, NotFoundException;
    void deleteRelationship(long id) throws IOException, NotFoundException;
}