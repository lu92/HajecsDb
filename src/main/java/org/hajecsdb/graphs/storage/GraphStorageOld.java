package org.hajecsdb.graphs.storage;


import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.PropertyContainer;

import java.io.IOException;

public interface GraphStorageOld {
    // CRUD's operations implementation
    GraphJsonTemplate saveGraph(Graph graph) throws IOException;
    Graph loadGraph(String filename) throws IOException;
    void saveNode(Entity entity);
//    Entity read(long id);
//    void update(Entity entity);
//    Entity delete(long id);
}
