package org.hajecsdb.graphs.storage;

import com.google.gson.GsonBuilder;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.hajecsdb.graphs.impl.NodeImpl;

import java.util.Set;

public class JsonToGraphMapperImpl implements JsonToGraphMapper {

    private GsonBuilder gsonBuilder;

    public JsonToGraphMapperImpl() {
        gsonBuilder = new GsonBuilder();
    }

    @Override
    public Graph mapToGraph(GraphJsonTemplate graphJsonTemplate) {
        String pathDir = (String) graphJsonTemplate.getGraphProperties().getProperty("pathDir").get().getValue();
        String graphName = (String) graphJsonTemplate.getGraphProperties().getProperty("graphName").get().getValue();
        Graph graph = new GraphImpl(pathDir, graphName);
        return graph;
    }

    public Node mapToNode(Properties nodeProperties) {
//        long nodeId = (long) nodeProperties.getProperty("id").get().getValue();
//        Set<String> keys = nodeProperties.getKeys();
//        keys.remove("id");
//        nodeProperties.getProperties(keys);
//        Node node = new NodeImpl(nodeId, nodeProperties.getProperties(keys));
//
//        return node;
        return null;
    }
}
