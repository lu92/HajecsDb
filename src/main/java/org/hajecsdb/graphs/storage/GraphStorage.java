package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.Graph;

import java.io.IOException;

public interface GraphStorage {
    void saveGraph(Graph graph) throws IOException;
    Graph loadGraph(String filename) throws IOException;
    void createNode(Entity entity) throws IOException;
    Entity readNode(long id) throws IOException;
    void updateNode(Entity entity);
    Entity deleteNode(long id);
    void createRelationship(Entity entity);
    Entity readRelationship(long id);
    void updateRelationship(Entity entity);
    Entity deleteRelationship(long id);
}
