package HajecsDb.unitTests.graphSerialization;

import HajecsDb.unitTests.utils.FileUtils;
import HajecsDb.unitTests.utils.NodeComparator;
import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.storage.BinaryGraphStorage;
import org.hajecsdb.graphs.storage.GraphStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.INT;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class BinaryGraphStorageTest {

    private GraphStorage graphStorage = new BinaryGraphStorage();
    private NodeComparator nodeComparator = new NodeComparator();

    @Before
    public void setup() {
        FileUtils.clearFile("nodes.bin");
        FileUtils.clearFile("nodesMetaData.bin");
        FileUtils.clearFile("relationshipMetaData.bin");
        FileUtils.clearFile("relationship.bin");
        FileUtils.clearFile("graph.bin");
    }

    @Test
    public void saveAndLoadGraphWithThreeNodesTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        Node node1 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        Node node2 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "Sarah", STRING)
                .add("age", 23, INT));

        Node node3 = graph.createNode(new Label("Person"), new Properties()
                .add("firstName", "Henry", STRING)
                .add("height", 180, INT)
                .add("age", 30, INT));

        // when
        graphStorage.saveGraph(graph);

        // then
        Graph loadedGraph = graphStorage.loadGraph("/home/test");
        assertThat(loadedGraph).isNotNull();
        assertThat(loadedGraph.getAllNodes()).hasSize(3);
        nodeComparator.isSame(node1, graph.getNodeById(1).get());
        nodeComparator.isSame(node2, graph.getNodeById(2).get());
        nodeComparator.isSame(node3, graph.getNodeById(3).get());
        assertThat(loadedGraph.getAllRelationships()).isEmpty();
    }

    @Test
    public void saveAndLoadThreeNodesAndTwoRelationshipsTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        Node node1 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        Node node2 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "Sarah", STRING)
                .add("age", 23, INT));

        Node node3 = graph.createNode(new Label("Person"), new Properties()
                .add("firstName", "Henry", STRING)
                .add("height", 180, INT)
                .add("age", 30, INT));

        graph.createRelationship(node1, node2, new Label("LIKES"));
        graph.createRelationship(node2, node3, new Label("KNOWS"));

        // when
        graphStorage.saveGraph(graph);

        // then
        Graph loadedGraph = graphStorage.loadGraph("/home/test");
        assertThat(loadedGraph).isNotNull();
        assertThat(loadedGraph.getAllNodes()).hasSize(3);
        nodeComparator.isSame(node1, graph.getNodeById(1).get());
        nodeComparator.isSame(node2, graph.getNodeById(2).get());
        nodeComparator.isSame(node3, graph.getNodeById(3).get());

        assertThat(loadedGraph.getAllRelationships()).hasSize(4);
        loadedGraph.findRelationship(node1.getId(), node2.getId(), Direction.OUTGOING, new Label("LIKES"));
        loadedGraph.findRelationship(node2.getId(), node1.getId(), Direction.INCOMING, new Label("LIKES"));
        loadedGraph.findRelationship(node2.getId(), node3.getId(), Direction.OUTGOING, new Label("KNOWS"));
        loadedGraph.findRelationship(node3.getId(), node2.getId(), Direction.INCOMING, new Label("KNOWS"));
    }

    @Test
    public void readNumberOfNodesInEmptyGraphTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        graphStorage.saveGraph(graph);

        // when
        long numberOfNodes = graphStorage.countNodes();

        // then
        assertThat(numberOfNodes).isEqualTo(0);
    }

    @Test
    public void readSeparatelyThreeNodesTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        Node node1 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        Node node2 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "Sarah", STRING)
                .add("age", 23, INT));

        Node node3 = graph.createNode(new Label("Person"), new Properties()
                .add("firstName", "Henry", STRING)
                .add("height", 180, INT)
                .add("age", 30, INT));

        // when
        graphStorage.saveGraph(graph);

        assertThat(graphStorage.countNodes()).isEqualTo(3);
        assertThat(nodeComparator.isSame(node1, graphStorage.readNode(1l).get()));
        assertThat(nodeComparator.isSame(node2, graphStorage.readNode(2l).get()));
        assertThat(nodeComparator.isSame(node3, graphStorage.readNode(3l).get()));
    }

    @Test
    public void createAndDeleteOneNodeTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        graphStorage.saveGraph(graph);
        assertThat(graphStorage.countNodes()).isEqualTo(0);

        Node node = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        graphStorage.saveNode(node);
        assertThat(graphStorage.countNodes()).isEqualTo(1);

        // when
        graphStorage.deleteNode(1);

        assertThat(graphStorage.readNode(1).isPresent()).isFalse();
        assertThat(graphStorage.countNodes()).isEqualTo(0);
    }

    @Test
    public void createAndDeleteThreeNodesTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        graphStorage.saveGraph(graph);

        Node node1 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        Node node2 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "Sarah", STRING)
                .add("age", 23, INT));

        Node node3 = graph.createNode(new Label("Person"), new Properties()
                .add("firstName", "Henry", STRING)
                .add("height", 180, INT)
                .add("age", 30, INT));

        graphStorage.saveNode(node1);
        graphStorage.saveNode(node2);
        graphStorage.saveNode(node3);
        assertThat(graphStorage.countNodes()).isEqualTo(3);

        // when
        graphStorage.deleteNode(1);

        assertThat(graphStorage.countNodes()).isEqualTo(2);
        assertThat(graphStorage.readNode(1).isPresent()).isFalse();
        assertThat(nodeComparator.isSame(node2, graphStorage.readNode(2).get()));
        assertThat(nodeComparator.isSame(node3, graphStorage.readNode(3).get()));

        // when
        graphStorage.deleteNode(2);

        assertThat(graphStorage.countNodes()).isEqualTo(1);
        assertThat(graphStorage.readNode(1).isPresent()).isFalse();
        assertThat(graphStorage.readNode(2).isPresent()).isFalse();
        assertThat(nodeComparator.isSame(node3, graphStorage.readNode(3).get()));

        // when
        graphStorage.deleteNode(3);

        assertThat(graphStorage.countNodes()).isEqualTo(0);
        assertThat(graphStorage.readNode(1).isPresent()).isFalse();
        assertThat(graphStorage.readNode(2).isPresent()).isFalse();
        assertThat(graphStorage.readNode(3).isPresent()).isFalse();
    }

    @Test
    public void createAndUpdateOneNodeTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        graphStorage.saveGraph(graph);

        Node node = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        graphStorage.saveNode(node);
        assertThat(graphStorage.countNodes()).isEqualTo(1);

        // when
        node.setProperty("firstName", "Victor");
        node.setProperty("age", 30);
        graphStorage.updateNode(node);


        // then
        assertThat(graphStorage.countNodes()).isEqualTo(1);
        Node fetchedNode = graphStorage.readNode(1).get();
        assertThat(fetchedNode.getAllProperties().getAllProperties()).containsOnly(
                new Property("id", LONG, 1l),
                new Property("label", STRING, "Student"),
                new Property("firstName", STRING, "Victor"),
                new Property("age", INT, 30)
        );
    }

    @Test
    public void createAndUpdateTwoNodeTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        Node node1 = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        Node node2 = graph.createNode(new Label("Person"), new Properties()
                .add("firstName", "Sarah", STRING)
                .add("age", 23, INT));

        graphStorage.saveGraph(graph);
        assertThat(graphStorage.countNodes()).isEqualTo(2);

        // when
        node1.setProperty("firstName", "Victor");
        node1.setProperty("age", 30);
        graphStorage.updateNode(node1);

        node2.setProperty("age", 18);
        graphStorage.updateNode(node2);


        // then
        assertThat(graphStorage.countNodes()).isEqualTo(2);
        Node fetchedNode1 = graphStorage.readNode(1).get();
        assertThat(fetchedNode1.getAllProperties().getAllProperties()).containsOnly(
                new Property("id", LONG, 1l),
                new Property("label", STRING, "Student"),
                new Property("firstName", STRING, "Victor"),
                new Property("age", INT, 30)
        );

        Node fetchedNode2 = graphStorage.readNode(2).get();
        assertThat(fetchedNode2.getAllProperties().getAllProperties()).containsOnly(
                new Property("id", LONG, 2l),
                new Property("label", STRING, "Person"),
                new Property("firstName", STRING, "Sarah"),
                new Property("age", INT, 18)
        );
    }

    @Test
    public void createAndAddNewPropertyToNodeTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        Node node = graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        graphStorage.saveGraph(graph);
        assertThat(graphStorage.countNodes()).isEqualTo(1);

        // when
        node.getAllProperties().add(new Property("lastName", STRING, "Connor"));
        graphStorage.updateNode(node);


        // then
        assertThat(graphStorage.countNodes()).isEqualTo(1);
        Node fetchedNode = graphStorage.readNode(1).get();
        assertThat(fetchedNode.getAllProperties().getAllProperties()).containsOnly(
                new Property("id", LONG, 1l),
                new Property("label", STRING, "Student"),
                new Property("firstName", STRING, "John"),
                new Property("lastName", STRING, "Connor"),
                new Property("age", INT, 25)
        );
    }

    @Test
    public void createAndReadRelationshipTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        Node node1 = graph.createNode(new Properties()
                .add("name", "first", STRING));
        Node node2 = graph.createNode(new Properties()
                .add("name", "second", STRING));

        graphStorage.saveGraph(graph);
        assertThat(graphStorage.countNodes()).isEqualTo(2);

        // when
        Relationship relationship = graph.createRelationship(node1, node2, new Label("CONNECTED"));
        graphStorage.saveRelationship(relationship);

        // then
        assertThat(graphStorage.countNodes()).isEqualTo(2);
        assertThat(graphStorage.countRelationships()).isEqualTo(2);

        Relationship fetchedRelationship = graphStorage.readRelationship(relationship.getId()).get();
        assertThat(nodeComparator.isSame(node1, fetchedRelationship.getStartNode())).isTrue();
        assertThat(nodeComparator.isSame(node2, fetchedRelationship.getEndNode())).isTrue();
        assertThat(fetchedRelationship.getId()).isEqualTo(3);
        assertThat(fetchedRelationship.getLabel()).isEqualTo(new Label("CONNECTED"));
        assertThat(fetchedRelationship.getDirection()).isEqualTo(Direction.OUTGOING);

        Relationship fetchedRelationship2 = graphStorage.readRelationship(relationship.getId()+1).get();
        assertThat(nodeComparator.isSame(node2, fetchedRelationship2.getStartNode())).isTrue();
        assertThat(nodeComparator.isSame(node1, fetchedRelationship2.getEndNode())).isTrue();
        assertThat(fetchedRelationship2.getId()).isEqualTo(4);
        assertThat(fetchedRelationship2.getLabel()).isEqualTo(new Label("CONNECTED"));
        assertThat(fetchedRelationship2.getDirection()).isEqualTo(Direction.INCOMING);
    }

    @Test
    public void createAndDeleteRelationshipTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        Node node1 = graph.createNode(new Properties()
                .add("name", "first", STRING));
        Node node2 = graph.createNode(new Properties()
                .add("name", "second", STRING));

        graphStorage.saveGraph(graph);
        assertThat(graphStorage.countNodes()).isEqualTo(2);

        Relationship relationship = graph.createRelationship(node1, node2, new Label("CONNECTED"));
        graphStorage.saveRelationship(relationship);
        assertThat(graphStorage.countRelationships()).isEqualTo(2);

        // when
        graphStorage.deleteRelationship(relationship.getId());

        // then
        assertThat(graphStorage.countRelationships()).isEqualTo(0);

        assertThat(graphStorage.readNode(1).get().getRelationships()).isEmpty();
        assertThat(graphStorage.readNode(2).get().getRelationships()).isEmpty();
    }


    @Test
    public void createAndChangeLabelOfRelationshipTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        Node node1 = graph.createNode(new Properties()
                .add("name", "first", STRING));
        Node node2 = graph.createNode(new Properties()
                .add("name", "second", STRING));

        graphStorage.saveGraph(graph);
        assertThat(graphStorage.countNodes()).isEqualTo(2);

        Relationship relationship = graph.createRelationship(node1, node2, new Label("CONNECTED"));
        graphStorage.saveRelationship(relationship);
        assertThat(graphStorage.countRelationships()).isEqualTo(2);

        relationship.setLabel(new Label("CHANGED"));

        // when
        graphStorage.updateRelationship(relationship);

        // then
        assertThat(graphStorage.countRelationships()).isEqualTo(2);

        Relationship fetchedRelationship = graphStorage.readRelationship(relationship.getId()).get();
        assertThat(nodeComparator.isSame(node1, fetchedRelationship.getStartNode())).isTrue();
        assertThat(nodeComparator.isSame(node2, fetchedRelationship.getEndNode())).isTrue();
        assertThat(fetchedRelationship.getId()).isEqualTo(3);
        assertThat(fetchedRelationship.getLabel()).isEqualTo(new Label("CHANGED"));
        assertThat(fetchedRelationship.getDirection()).isEqualTo(Direction.OUTGOING);

        Relationship fetchedRelationship2 = graphStorage.readRelationship(relationship.getId()+1).get();
        assertThat(nodeComparator.isSame(node2, fetchedRelationship2.getStartNode())).isTrue();
        assertThat(nodeComparator.isSame(node1, fetchedRelationship2.getEndNode())).isTrue();
        assertThat(fetchedRelationship2.getId()).isEqualTo(4);
        assertThat(fetchedRelationship2.getLabel()).isEqualTo(new Label("CHANGED"));
        assertThat(fetchedRelationship2.getDirection()).isEqualTo(Direction.INCOMING);
    }
}
