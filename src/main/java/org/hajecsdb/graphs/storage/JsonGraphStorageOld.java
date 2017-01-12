package org.hajecsdb.graphs.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.Graph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JsonGraphStorageOld implements GraphStorageOld {

    private GraphToJsonMapper graphToJsonMapper;
    private JsonToGraphMapper jsonToGraphMapper;
    private GsonBuilder gsonBuilder;

    public JsonGraphStorageOld() {
        this.graphToJsonMapper = new GraphToJsonMapperImpl();
        this.jsonToGraphMapper = new JsonToGraphMapperImpl();
        this.gsonBuilder = new GsonBuilder();
        gsonBuilder.setPrettyPrinting();
    }

    @Override
    public GraphJsonTemplate saveGraph(Graph graph) throws IOException {
        Gson gson = gsonBuilder.create();
        GraphJsonTemplate graphJsonTemplate = graphToJsonMapper.mapToGraphJsonTemplate(graph);
        Path path = Paths.get(graph.getFilename());
        Files.write(path, gson.toJson(graphJsonTemplate).getBytes());
        System.out.println(gson.toJson(graphJsonTemplate));
        return graphJsonTemplate;
    }

    @Override
    public Graph loadGraph(String filename) throws IOException {
        Path filePath = FileSystems.getDefault().getPath(".", filename);
        InputStream inputStream = Files.newInputStream(filePath);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream));
        String line = null;
        String jsonFigure = "";
        while ((line = bufferedReader.readLine()) != null) {
            jsonFigure += line;
        }
        Gson gson = gsonBuilder.create();
        GraphJsonTemplate graphJsonTemplate = gson.fromJson(jsonFigure, GraphJsonTemplate.class);
        Graph graph = jsonToGraphMapper.mapToGraph(graphJsonTemplate);
        return graph;
    }

    @Override
    public void saveNode(Entity entity) {

    }


}
