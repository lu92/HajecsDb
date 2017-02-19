package HajecsDb.graphSerialization;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.impl.NodeImpl;
import org.hajecsdb.graphs.impl.RelationshipImpl;
import org.hajecsdb.graphs.storage.entities.BinaryRelationship;
import org.hajecsdb.graphs.storage.serializers.RelationshipSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.INT;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class RelationshipSerializerTest {

    private String relationshipFilename = "relationship.bin";
    private String relationshipMetadataFilename = "relationshipMetaData.bin";
    private RelationshipSerializer relationshipSerializer = new RelationshipSerializer(relationshipFilename, relationshipMetadataFilename);

    private Node startNode = new NodeImpl(100);
    private Node endNode = new NodeImpl(200);

    @Before
    public void before() throws IOException {
        clearContentFile(relationshipFilename);
        clearContentFile(relationshipMetadataFilename);
    }

    @Test
    public void saveAndReadRelationshipTest() throws IOException {
        // given
        Properties expectedProperties = new Properties()
                .add(new Property("id", LONG, 1l))
                .add(new Property("startNode", LONG, startNode.getId()))
                .add(new Property("endNode", LONG, endNode.getId()))
                .add(new Property("RelationshipType", STRING, "CONNECTED"))
                .add(new Property("direction", STRING, Direction.INCOMING.toString()));

        Relationship relationship = new RelationshipImpl(1, startNode, endNode, Direction.INCOMING, new RelationshipType("CONNECTED"));

        // when
        relationshipSerializer.save(relationship);
        Optional<Relationship> relationshipOptional = relationshipSerializer.read(1);

        // then
        assertThat(relationshipSerializer.count()).isEqualTo(1);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getAllProperties().getAllProperties()).hasSize(5);
        assertThat(relationshipOptional.get().getAllProperties()).isEqualTo(expectedProperties);
    }

    @Test
    public void saveTwoRelationshipsAndReadAllTest() throws IOException {
        Relationship relationship1 = new RelationshipImpl(1, startNode, endNode, Direction.INCOMING, new RelationshipType("CONNECTED"));
        Relationship relationship2 = new RelationshipImpl(2, endNode, startNode, Direction.INCOMING, new RelationshipType("KNOWS"));

        // when
        relationshipSerializer.save(relationship1);
        relationshipSerializer.save(relationship2);
        List<Relationship> relationships = relationshipSerializer.readAll();
        // then
        assertThat(relationshipSerializer.count()).isEqualTo(2);
        assertThat(relationships).hasSize(2);
    }

    @Test
    public void saveAndDeleteRelationshipTest() throws IOException {
        // given
        Relationship relationship = new RelationshipImpl(1, startNode, endNode, Direction.INCOMING, new RelationshipType("CONNECTED"));

        // when
        relationshipSerializer.save(relationship);
        relationshipSerializer.delete(1);

        // then
        assertThat(relationshipSerializer.count()).isEqualTo(0);
        assertThat(relationshipSerializer.readAll()).hasSize(0);
    }

    @Test
    public void saveAndUpdateRelationshipTest() throws IOException, NotFoundException {
        // given
        Relationship relationship = new RelationshipImpl(1, startNode, endNode, Direction.INCOMING, new RelationshipType("CONNECTED"));
        relationshipSerializer.save(relationship);

        // when
        relationship.getAllProperties().add("length", 10, INT);
        relationshipSerializer.update(relationship);

        // then
        Optional<Relationship> relationshipOptional = relationshipSerializer.read(1);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getProperty("length").get()).isEqualTo(new Property("length", INT, 10));

    }

    private void clearContentFile(String filename) throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(filename);
        pw.close();
    }

}
