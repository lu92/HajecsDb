package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.NotFoundException;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.storage.entities.BinaryEntity;

import java.io.IOException;
import java.util.Optional;

public interface GraphStorage {
    void saveGraph(Graph graph) throws IOException;
    Graph loadGraph(String filename) throws IOException;
    BinaryEntity saveNode(Node node) throws IOException;
    Optional<Node> readNode(long id) throws IOException;
    void updateNode(Node node) throws IOException, NotFoundException;
    void deleteNode(long id) throws IOException, NotFoundException;
    long countNodes() throws IOException;
    BinaryEntity saveRelationship(Relationship relationship) throws IOException;
    Optional<Relationship> readRelationship(long id) throws IOException;
    void updateRelationship(Relationship relationship) throws IOException, NotFoundException;
    void deleteRelationship(long id) throws IOException, NotFoundException;
    long countRelationships() throws IOException;
}
