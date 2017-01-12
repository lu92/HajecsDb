package HajecsDb;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.hajecsdb.graphs.storage.GraphStorageOld;
import org.hajecsdb.graphs.storage.JsonGraphStorageOld;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class GraphStorageOldTest {

    @Test
    public void saveAndLoadGraph() throws IOException {
        Graph graph = new GraphImpl("/Users/Lucjan", "testDb");
        Node henry = graph.createNode(new Properties()
                .add("firstName", "Henry", STRING).add("age", 25, LONG));
        Node lisa = graph.createNode(new Properties()
                .add("firstName", "Lisa", STRING).add("age", 18, LONG));
        Node brian = graph.createNode(new Properties()
                .add("firstName", "Monika", STRING).add("age", 18, LONG));
        graph.createRelationship(henry, "LIKES", lisa);
        graph.createRelationship(lisa, "HATES", henry);
        graph.createRelationship(lisa, "KNOWS", brian);
        graph.createRelationship(brian, "KNOWS", henry);

        GraphStorageOld graphStorageOld = new JsonGraphStorageOld();
        graphStorageOld.saveGraph(graph);
    }
}
