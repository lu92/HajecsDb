package org.hajecsdb.graphs.storage;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.storage.entities.BinaryEntity;
import org.hajecsdb.graphs.storage.mappers.PropertiesBinaryMapper;
import org.hajecsdb.graphs.storage.serializers.NodeSerializer;
import org.hajecsdb.graphs.storage.serializers.RelationshipSerializer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
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
//        final String pathDir = "/home";
        final String graphName = "test";
        RandomAccessFile graphDataAccessFile = new RandomAccessFile(graphFilename, "rw");
        graphDataAccessFile.seek(0);
        byte[] binaryPathDir = new byte[120];
        graphDataAccessFile.read(binaryPathDir);
        String pathDir = ByteUtils.bytesToString(binaryPathDir);
        System.out.println("loaded pathDir: " + pathDir);


        Graph graph = new GraphImpl(pathDir, graphName);

        graph.getAllNodes().addAll(nodeSerializer.readAll());

        List<Relationship> relationships = relationshipSerializer.readAll();
        for (Relationship relationship : relationships) {
            relationship.setId((Long)relationship.getProperty("id").get().getValue());
            Node startNode = graph.getNodeById((long) relationship.getProperty("startNode").get().getValue()).get();
            relationship.setStartNode(startNode);
            Node endNode = graph.getNodeById((long) relationship.getProperty("endNode").get().getValue()).get();
            relationship.setEndNode(endNode);
            relationship.setLabel(new Label((String) relationship.getProperty("label").get().getValue()));
            relationship.setDirection(Direction.valueOf((String) relationship.getProperty("direction").get().getValue()));
            startNode.getRelationships().add(relationship);
        }
        graph.getAllRelationships().addAll(relationships);
        graphDataAccessFile.close();

        return graph;
    }

    @Override
    public BinaryEntity saveNode(Node node) throws IOException {
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
    public long countNodes() throws IOException {
        return nodeSerializer.count();
    }

    @Override
    public BinaryEntity saveRelationship(Relationship relationship) throws IOException {
        BinaryEntity binaryRelationship = relationshipSerializer.save(relationship);
        Relationship revertedRelationship = relationship.reverse();
        revertedRelationship.setId(relationship.getId()+1);
        BinaryEntity binaryRelationship2 = relationshipSerializer.save(revertedRelationship);
        return binaryRelationship;
    }

    @Override
    public Optional<Relationship> readRelationship(long id) throws IOException {
        Optional<Relationship> fetchedRelationship = relationshipSerializer.read(id);

        if (fetchedRelationship.isPresent()) {
            // load relationship's start and end nodes
            Node startNode = nodeSerializer.read((Long) fetchedRelationship.get().getProperty("startNode").get().getValue()).get();
            Node endNode = nodeSerializer.read((Long) fetchedRelationship.get().getProperty("endNode").get().getValue()).get();

            fetchedRelationship.get().setId((Long)fetchedRelationship.get().getProperty("id").get().getValue());
            fetchedRelationship.get().setLabel(new Label((String) fetchedRelationship.get().getProperty("label").get().getValue()));
            fetchedRelationship.get().setStartNode(startNode);
            fetchedRelationship.get().setEndNode(endNode);
            fetchedRelationship.get().setDirection(Direction.valueOf((String) fetchedRelationship.get().getProperty("direction").get().getValue()));
        }
        return fetchedRelationship;
    }

    @Override
    public void updateRelationship(Relationship relationship) throws IOException, NotFoundException {
        relationshipSerializer.update(relationship);
        Relationship revertedRelationship = relationship.reverse();
        revertedRelationship.setId(relationship.getId()+1);
        relationshipSerializer.update(revertedRelationship);
    }

    @Override
    public void deleteRelationship(long id) throws IOException, NotFoundException {
        relationshipSerializer.delete(id);
        relationshipSerializer.delete(id+1);
    }

    @Override
    public long countRelationships() throws IOException {
        return relationshipSerializer.count();
    }
}
