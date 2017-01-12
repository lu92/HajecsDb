package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Graph;

public interface JsonToGraphMapper {
    Graph mapToGraph(GraphJsonTemplate graphJsonTemplate);
}
