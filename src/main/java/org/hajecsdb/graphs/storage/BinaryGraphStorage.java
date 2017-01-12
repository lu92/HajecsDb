package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.storage.serializers.EntitySerializer;

import java.io.IOException;
import java.nio.file.Path;


public class BinaryGraphStorage implements GraphStorage {

    private EntityMapper entityMapper = new EntityMapper();
    private EntitySerializer entitySerializer = new EntitySerializer();
    private Path nodesPath;
    private Path nodesMetaDataPath;

    public BinaryGraphStorage(Path nodesPath) {
        this.nodesPath = nodesPath;
    }

    @Override
    public void saveGraph(Graph graph) throws IOException {

    }

    @Override
    public Graph loadGraph(String filename) throws IOException {
        return null;
    }

    @Override
    public void createNode(Entity entity) throws IOException {
        entitySerializer.serializeEntity(nodesPath, nodesMetaDataPath, entity, EntityType.NODE);
    }

    @Override
    public Entity readNode(long id) throws IOException {
        Properties properties = entitySerializer.deserializeEntitiesProperties(nodesPath, id, EntityType.NODE);
        Node node = entityMapper.toNode(properties);
        return node;
    }

    @Override
    public void updateNode(Entity entity) {

    }

    @Override
    public Entity deleteNode(long id) {
        return null;
    }

    @Override
    public void createRelationship(Entity entity) {

    }

    @Override
    public Entity readRelationship(long id) {
        return null;
    }

    @Override
    public void updateRelationship(Entity entity) {

    }

    @Override
    public Entity deleteRelationship(long id) {
        return null;
    }
}
