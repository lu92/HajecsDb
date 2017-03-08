package HajecsDb.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.*;

@RunWith(MockitoJUnitRunner.class)
public class CreateNodeTest {

    private Graph graph;
    private CypherExecutor cypherExecutor;

    @Test
    public void createEmptyNodeWithLabelTest() {
        // given
        String command = "CREATE (n: Person)";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).isEmpty();
        assertThat(graph.getAllNodes().size()).isEqualTo(1);
        Node fetchedNode = graph.getNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l), new Property("label", STRING, "Person"));
    }

    @Test
    public void createNodeWithIntParameterTest() {
        // given
        String command = "CREATE (n: Person {age: 25})";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).isEmpty();
        assertThat(graph.getAllNodes().size()).isEqualTo(1);
        Node fetchedNode = graph.getNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("age", INT, 25));

    }

    @Test
    public void createNodeWithLongParameterTest() {
    }

    @Test
    public void createNodeWithFloatParameterTest() {
    }

    @Test
    public void createNodeWithStringParameterTest() {
        // given
        String command = "CREATE (n: Person {name: 'Peter'})";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).isEmpty();
        assertThat(graph.getAllNodes().size()).isEqualTo(1);
        Node fetchedNode = graph.getNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "Peter"));
    }

    @Test
    public void createNodeWithTwoParametersTest() {
        // given
        String command = "CREATE (n: Person {firstName: 'Jan', lastName: 'Kowalski'})";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).isEmpty();
        assertThat(graph.getAllNodes().size()).isEqualTo(1);
        Node fetchedNode = graph.getNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties()).hasSize(4);
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("firstName", STRING, "Jan"),
                        new Property("lastName", STRING, "Kowalski"));
    }

    @Test
    public void createNodeWithTreeParametersTest() {
        // given
        String command = "CREATE (n: Person {firstName: 'Jan', salary: 3000.00, age: 40})";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).isEmpty();
        assertThat(graph.getAllNodes().size()).isEqualTo(1);
        Node fetchedNode = graph.getNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties()).hasSize(5);
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("firstName", STRING, "Jan"),
                        new Property("salary", DOUBLE, 3000.00),
                        new Property("age", INT, 40));
    }
}
