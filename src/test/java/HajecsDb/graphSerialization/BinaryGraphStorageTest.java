package HajecsDb.graphSerialization;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.hajecsdb.graphs.storage.BinaryGraphStorage;
import org.hajecsdb.graphs.storage.GraphStorage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import javax.print.DocFlavor;
import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.INT;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class BinaryGraphStorageTest {

    GraphStorage graphStorage = new BinaryGraphStorage();

    @Test
    public void saveGraphTest() throws IOException {
        // given
        GraphImpl graph = new GraphImpl("/home", "test");
        graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "John", STRING)
                .add("age", 25, INT));

        graph.createNode(new Label("Student"), new Properties()
                .add("firstName", "Sarah", STRING)
                .add("age", 23, INT));

        graph.createNode(new Label("Person"), new Properties()
                .add("firstName", "Henry", STRING)
                .add("height", 180, INT)
                .add("age", 30, INT));

        // when
        graphStorage.saveGraph(graph);

        // then
//        graphStorage.loadGraph("");
    }
}
