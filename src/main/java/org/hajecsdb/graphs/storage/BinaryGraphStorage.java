package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.NotFoundException;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.storage.entities.BinaryNode;
import org.hajecsdb.graphs.storage.entities.BinaryRelationship;
import org.hajecsdb.graphs.storage.mappers.PropertiesBinaryMapper;
import org.hajecsdb.graphs.storage.serializers.NodeSerializer;
import org.hajecsdb.graphs.storage.serializers.RelationshipSerializer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;


public class BinaryGraphStorage implements GraphStorage {
    private String nodesFilename = "nodes.bin";
    private String nodeMetadataFilename = "nodesMetaData.bin";

    private String relationshipsFilename = "relationship.bin";
    private String relationshipMetadataFilename = "relationshipMetaData.bin";

    private String graphFilename = "graph.bin";

    private NodeSerializer nodeSerializer = new NodeSerializer(nodesFilename, nodeMetadataFilename);
    private RelationshipSerializer relationshipSerializer= new RelationshipSerializer(relationshipsFilename, relationshipMetadataFilename);
    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

    @Override
    public void saveGraph(Graph graph) throws IOException {
        RandomAccessFile graphMetaDataAccessFile = new RandomAccessFile(graphFilename, "rw");
        graphMetaDataAccessFile.seek(0);
        graphMetaDataAccessFile.write(propertiesBinaryMapper.toBinaryFigure(graph.getProperties()).getBytes());
        graphMetaDataAccessFile.close();


        for (Node node : graph.getAllNodes()) {
            nodeSerializer.save(node);
        }

        for (Relationship relationship : graph.getAllRelationships()) {
            relationshipSerializer.save(relationship);
        }
    }

    @Override
    public Graph loadGraph(String filename) throws IOException {
        return null;
    }

    @Override
    public BinaryNode createNode(Node node) throws IOException {
        return nodeSerializer.save(node);
    }

    @Override
    public Optional<Node> readNode(long id) throws IOException {
        return nodeSerializer.read(id);
    }

    @Override
    public void updateNode(Node node) throws IOException, NotFoundException {
        nodeSerializer.update(node);
    }

    @Override
    public void deleteNode(long id) throws IOException, NotFoundException {
        nodeSerializer.delete(id);
    }

    @Override
    public BinaryRelationship createRelationship(Relationship relationship) throws IOException {
        return relationshipSerializer.save(relationship);
    }

    @Override
    public Optional<Relationship> readRelationship(long id) throws IOException {
        return relationshipSerializer.read(id);
    }

    @Override
    public void updateRelationship(Relationship relationship) throws IOException, NotFoundException {
        relationshipSerializer.update(relationship);
    }

    @Override
    public void deleteRelationship(long id) throws IOException, NotFoundException {
        relationshipSerializer.delete(id);
    }
}
