package HajecsDb.graphSerialization;

import HajecsDb.utils.NodeComparator;
import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.storage.BinaryGraphStorage;
import org.hajecsdb.graphs.storage.GraphStorage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.INT;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class BinaryGraphStorageTest {

    private GraphStorage graphStorage = new BinaryGraphStorage();
    private NodeComparator nodeComparator = new NodeComparator();

    @Test
    public void saveGraphWithThreeNodesTest() throws IOException {
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
//        assertThat(loadedGraph.getProperties().getProperty("lastGeneratedId").get()).isEqualTo(new Property())
    }
}
