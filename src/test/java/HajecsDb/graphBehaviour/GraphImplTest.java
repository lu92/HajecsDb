package HajecsDb.graphBehaviour;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.hajecsdb.graphs.impl.RelationshipImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;


@RunWith(MockitoJUnitRunner.class)
public class GraphImplTest {

    @Test
    public void pathDirAndNameTest() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        assertThat(graphImpl.getPathDir()).isEqualTo("/home");
        assertThat(graphImpl.getGraphName()).isEqualTo("test");
    }

    @Test()
    public void pathDirIsNullExpectedException() {
        try {
            new GraphImpl(null, "test");
        } catch (NullPointerException e) {
            assertThat(e.getMessage()).isEqualTo("pathDir and graphName can't be empty or null");
        }
    }

    @Test
    public void nameIsNullExpectedException() {
        try {
            new GraphImpl("/home", null);
        } catch (NullPointerException e) {
            assertThat(e.getMessage()).isEqualTo("pathDir and graphName can't be empty or null");
        }
    }

    @Test
    public void createAndGetNodeTest() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        graphImpl.createNode();
        assertThat(graphImpl.getAllNodes()).hasSize(1);
        assertThat(graphImpl.getNodeById(1).isPresent()).isTrue();
        assertThat(graphImpl.getNodeById(1).get().hasLabel()).isFalse();
        assertThat(graphImpl.getNodeById(1).get().getDegree()).isEqualTo(0);
        assertThat(graphImpl.getNodeById(1).get().hasRelationship()).isFalse();
    }

    @Test
    public void createAndGetNodeWithLabel() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        graphImpl.createNode(new Label("Person"));
        assertThat(graphImpl.getAllNodes()).hasSize(1);
        assertThat(graphImpl.getNodeById(1).isPresent()).isTrue();
        assertThat(graphImpl.getNodeById(1).get().hasLabel()).isTrue();
        assertThat(graphImpl.getNodeById(1).get().getLabel().getName()).isEqualTo("Person");
        assertThat(graphImpl.getNodeById(1).get().getDegree()).isEqualTo(0);
        assertThat(graphImpl.getNodeById(1).get().hasRelationship()).isFalse();
    }

    @Test
    public void createNodeWithProperties() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Properties properties = new Properties().add("name", "Lucian", STRING).add("age", 25, LONG);
        graphImpl.createNode(properties);
        assertThat(graphImpl.getAllNodes()).hasSize(1);
        assertThat(graphImpl.getNodeById(1).isPresent()).isTrue();
        assertThat(graphImpl.getNodeById(1).get().hasLabel()).isFalse();
        assertThat(graphImpl.getNodeById(1).get().getDegree()).isEqualTo(0);
        assertThat(graphImpl.getNodeById(1).get().hasRelationship()).isFalse();
        assertThat(graphImpl.getNodeById(1).get().getAllProperties().size()).isEqualTo(3);
        assertThat(graphImpl.getNodeById(1).get().getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat(graphImpl.getNodeById(1).get().getProperty("name").get().getValue()).isEqualTo("Lucian");
        assertThat(graphImpl.getNodeById(1).get().getProperty("age").get().getValue()).isEqualTo(25);
    }

    @Test
    public void createNodeWithLabelAndProperties() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Properties properties = new Properties().add("name", "Lucian", STRING).add("age", 25, LONG);
        graphImpl.createNode(new Label("Person"), properties);
        assertThat(graphImpl.getAllNodes()).hasSize(1);
        assertThat(graphImpl.getNodeById(1).isPresent()).isTrue();
        assertThat(graphImpl.getNodeById(1).get().hasLabel()).isTrue();
        assertThat(graphImpl.getNodeById(1).get().getDegree()).isEqualTo(0);
        assertThat(graphImpl.getNodeById(1).get().hasRelationship()).isFalse();
        assertThat(graphImpl.getNodeById(1).get().getAllProperties().size()).isEqualTo(4);
        assertThat(graphImpl.getNodeById(1).get().getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat(graphImpl.getNodeById(1).get().getProperty("name").get().getValue()).isEqualTo("Lucian");
        assertThat(graphImpl.getNodeById(1).get().getProperty("age").get().getValue()).isEqualTo(25);
        assertThat(graphImpl.getNodeById(1).get().getProperty("label").get().getValue()).isEqualTo("Person");
    }

    @Test
    public void getAllNodes() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        graphImpl.createNode();
        graphImpl.createNode();
        graphImpl.createNode();
        assertThat(graphImpl.getAllNodes()).hasSize(3);
        assertThat(graphImpl.getAllNodes()).containsOnly(
                graphImpl.getNodeById(1).get(),
                graphImpl.getNodeById(2).get(),
                graphImpl.getNodeById(3).get());
    }

    @Test
    public void changeNodesProperty() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        graphImpl.createNode(new Label("Person"), new Properties().add("name", "James", STRING));
        assertThat(graphImpl.getNodeById(1).get().getProperty("name").get().getValue()).isEqualTo("James");
        graphImpl.getNodeById(1l).get().setProperties(new Properties().add("name", "Peter", STRING));
        assertThat(graphImpl.getNodeById(1).get().getProperty("name").get().getValue()).isEqualTo("Peter");
    }

    @Test
    public void deleteNodesProperty() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        graphImpl.createNode(new Label("Person"), new Properties().add("name", "James", STRING));
        assertThat(graphImpl.getNodeById(1).get().getProperty("name").get().getValue()).isEqualTo("James");

        Properties properties = graphImpl.getNodeById(1).get().deleteProperties("name");
        assertThat(graphImpl.getNodeById(1).get().hasProperty("name")).isFalse();

        assertThat(properties.size()).isEqualTo(1);
        assertThat(properties.hasProperty("name")).isTrue();
        assertThat(properties.getProperty("name").get().getValue()).isEqualTo("James");
    }

    @Test
    public void deleteSeparatedNode() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node node = graphImpl.createNode();
        assertThat(graphImpl.getAllNodes()).hasSize(1);
        Node deletedNode = graphImpl.deleteNode(node.getId());
        assertThat(deletedNode.getId()).isEqualTo(1);
        assertThat(graphImpl.getAllNodes()).hasSize(0);
    }

    @Test
    public void deleteNodeWhichHasRelationshipExpectedException() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        try {
            graphImpl.deleteNode(1);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Node has relationship!");
        }
    }

    @Test
    public void deleteDefunctNodeExpectedException() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        try {
            graphImpl.deleteNode(-1);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Node does not exist!");
        }
    }

    @Test
    public void deleteNodeWithRelationshipExpectedException() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        try {
            graphImpl.deleteNode(firstNode.getId());
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Node has relationship!");
        }
    }

    @Test
    public void createAndGetRelationship() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        Relationship relationship = graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        assertThat(relationship.getStartNode().getId()).isEqualTo(1);
        assertThat(relationship.getEndNode().getId()).isEqualTo(2);

        assertThat(firstNode.getDegree()).isEqualTo(1);
        assertThat(firstNode.getRelationships()).hasSize(1);
        assertThat(firstNode.getRelationships(Direction.OUTGOING, new RelationshipType("KNOW"))).hasSize(1);

        assertThat(secondNode.getDegree()).isEqualTo(1);
        assertThat(secondNode.getRelationships()).hasSize(1);
        assertThat(secondNode.getRelationships(Direction.INCOMING, new RelationshipType("KNOW"))).hasSize(1);
    }

    @Test
    public void createRelationshipWhenExistExpectedException() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        try {
            graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Relationships already exists!");
        }
    }

    @Test
    public void addRelationshipWithoutTypeExpectedException() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        try {
            graphImpl.createRelationship(firstNode, secondNode, null);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Relationships type is null or empty!");
        }
    }

    @Test
    public void addRelationshipToNotExistedNodeExpectedException() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        try {
            graphImpl.createRelationship(firstNode, null, new RelationshipType("KNOW"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("One or both nodes don't exist!");
        }
    }

    @Test
    public void getAllRelationships() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        Node thirdNode = graphImpl.createNode();
        Node fourthNode = graphImpl.createNode();
        Relationship relationship1 = graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        Relationship relationship2 = graphImpl.createRelationship(secondNode, thirdNode, new RelationshipType("KNOW"));
        Relationship relationship3 = graphImpl.createRelationship(thirdNode, fourthNode, new RelationshipType("KNOW"));
        Relationship relationship4 = graphImpl.createRelationship(fourthNode, firstNode, new RelationshipType("KNOW"));
        Relationship relationship5 = graphImpl.createRelationship(firstNode, thirdNode, new RelationshipType("KNOW"));
        assertThat(graphImpl.getNodeById(1).get().getRelationships()).contains(
            new RelationshipImpl(relationship1.getId(), graphImpl.getNodeById(1).get(),
                    graphImpl.getNodeById(2).get(), Direction.OUTGOING, new RelationshipType("KNOW"))
        );
    }

    @Test
    public void getDegrees() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        Node thirdNode = graphImpl.createNode();
        Node fourthNode = graphImpl.createNode();
        graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        graphImpl.createRelationship(secondNode, thirdNode, new RelationshipType("KNOW"));
        graphImpl.createRelationship(thirdNode, fourthNode, new RelationshipType("KNOW"));
        graphImpl.createRelationship(fourthNode, firstNode, new RelationshipType("KNOW"));
        graphImpl.createRelationship(firstNode, thirdNode, new RelationshipType("KNOW"));
        assertThat(graphImpl.getNodeById(1).get().getDegree()).isEqualTo(3);
        assertThat(graphImpl.getNodeById(2).get().getDegree()).isEqualTo(2);
        assertThat(graphImpl.getNodeById(3).get().getDegree()).isEqualTo(3);
        assertThat(graphImpl.getNodeById(4).get().getDegree()).isEqualTo(2);
    }

    @Test
    public void getAllRelationshipTypes() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        Node thirdNode = graphImpl.createNode();
        Node fourthNode = graphImpl.createNode();
        graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        graphImpl.createRelationship(secondNode, thirdNode, new RelationshipType("WORK"));
        graphImpl.createRelationship(thirdNode, fourthNode, new RelationshipType("FRIEND"));
        graphImpl.createRelationship(fourthNode, firstNode, new RelationshipType("KNOW"));
        graphImpl.createRelationship(firstNode, thirdNode, new RelationshipType("FRIEND"));

        assertThat(graphImpl.getAllRelationshipTypes()).containsOnly(
                new RelationshipType("KNOW"),
                new RelationshipType("WORK"),
                new RelationshipType("FRIEND"));
    }

    @Test
    public void findRelationshipByType() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        Relationship knowRelationship = graphImpl.findRelationship(1, 2, new RelationshipType("KNOW"));
        assertThat(knowRelationship).isNotNull();
        assertThat(knowRelationship.getStartNode().getId()).isEqualTo(1);
        assertThat(knowRelationship.getEndNode().getId()).isEqualTo(2);
        assertThat(knowRelationship.getType()).isEqualTo(new RelationshipType("KNOW"));
    }

    @Test
    public void getRelationshipById() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        Relationship relationship = graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        Optional<Relationship> relationshipById = graphImpl.getRelationshipById(relationship.getId());

        assertThat(relationshipById.isPresent()).isTrue();
        assertThat(relationshipById.get().getStartNode().getId()).isEqualTo(1);
        assertThat(relationshipById.get().getEndNode().getId()).isEqualTo(2);
        assertThat(relationshipById.get().getType()).isEqualTo(new RelationshipType("KNOW"));
    }

    @Test
    public void setRelationshipBetweenSameNodeExpectedException() {
        try {
            GraphImpl graphImpl = new GraphImpl("/home", "test");
            Node firstNode = graphImpl.createNode();
            graphImpl.createRelationship(firstNode, firstNode, new RelationshipType("KNOW"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("");
        }
    }

    @Test
    public void deleteRelationship() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode();
        Node secondNode = graphImpl.createNode();
        Relationship relationship = graphImpl.createRelationship(firstNode, secondNode, new RelationshipType("KNOW"));
        graphImpl.deleteRelationship(relationship.getId());
        assertThat(firstNode.getDegree()).isEqualTo(0);
        assertThat(firstNode.getRelationships()).isEmpty();
        assertThat(secondNode.getDegree()).isEqualTo(0);
        assertThat(secondNode.getRelationships()).isEmpty();
        assertThat(graphImpl.getAllRelationships()).isEmpty();
    }

    @Test
    public void getAllPropertyKeys() {
        GraphImpl graphImpl = new GraphImpl("/home", "test");
        Node firstNode = graphImpl.createNode(new Properties()
                .add("firstName", "", STRING).add("lastName", "", STRING).add("age", 0, LONG));
        assertThat(graphImpl.getNodeById(1).get().getAllProperties().getKeys())
                .containsOnly("id", "firstName", "lastName", "age");
    }
}
