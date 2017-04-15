package HajecsDb.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class RemovePropertyTest {

    private Graph graph;
    private CypherExecutor cypherExecutor = new CypherExecutor();

    @Test
    public void removeSinglePropertyFromNodeTest() {
        // given
        String command = "MATCH (n) REMOVE n.age";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Properties().add("age", 40l, PropertyType.LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties removed: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(1);
        assertThat(graph.getNodeById(1).get().getAllProperties().size()).isEqualTo(1);
        assertThat((Long) graph.getNodeById(1).get().getAllProperties().getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) REMOVE n.age");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void removeSinglePropertyFromThreeNodeTest() {
        // given
        String command = "MATCH (n) REMOVE n.age";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Properties().add("age", 40l, PropertyType.LONG));
        graph.createNode(new Properties().add("age", 30l, PropertyType.LONG));
        graph.createNode(new Properties().add("age", 20l, PropertyType.LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties removed: 3");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(3);

        assertThat(graph.getNodeById(1).get().getAllProperties().size()).isEqualTo(1);
        assertThat((Long) graph.getNodeById(1).get().getAllProperties().getProperty("id").get().getValue()).isEqualTo(1l);

        assertThat(graph.getNodeById(2).get().getAllProperties().size()).isEqualTo(1);
        assertThat((Long) graph.getNodeById(2).get().getAllProperties().getProperty("id").get().getValue()).isEqualTo(2l);

        assertThat(graph.getNodeById(3).get().getAllProperties().size()).isEqualTo(1);
        assertThat((Long) graph.getNodeById(3).get().getAllProperties().getProperty("id").get().getValue()).isEqualTo(3l);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) REMOVE n.age");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void removeLabelFromNodeTest() {
        // given
        String command = "MATCH (n: Person) REMOVE n:Person";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Person"));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Labels removed: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(1);
        assertThat(graph.getNodeById(1).get().getAllProperties().size()).isEqualTo(1);
        assertThat(graph.getNodeById(1).get().getAllProperties().getProperty("label").isPresent()).isFalse();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) REMOVE n:Person");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void removeLabelFromThreeNodesTest() {
        // given
        String command = "MATCH (n: Person) REMOVE n:Person";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Person"));
        graph.createNode(new Label("Person"));
        graph.createNode(new Label("Person"));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Labels removed: 3");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(3);
        assertThat(graph.getNodeById(1).get().getAllProperties().size()).isEqualTo(1);
        assertThat(graph.getNodeById(1).get().getAllProperties().getProperty("label").isPresent()).isFalse();
        assertThat(graph.getNodeById(2).get().getAllProperties().size()).isEqualTo(1);
        assertThat(graph.getNodeById(2).get().getAllProperties().getProperty("label").isPresent()).isFalse();
        assertThat(graph.getNodeById(3).get().getAllProperties().size()).isEqualTo(1);
        assertThat(graph.getNodeById(3).get().getAllProperties().getProperty("label").isPresent()).isFalse();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) REMOVE n:Person");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void removeNotExistedPropertyFromNodeTest() {
        // given
        String command = "MATCH (n) REMOVE n.fakeProperty";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties removed: 0");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(1);
        assertThat(graph.getNodeById(1).get().getAllProperties().size()).isEqualTo(1);
        assertThat(graph.getNodeById(1).get().getAllProperties().getProperty("fakeProperty").isPresent()).isFalse();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) REMOVE n.fakeProperty");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void removeSinglePropertyFromMatchedNodeTest() {
        // given
        String command = "MATCH (n) WHERE n.age > 25 REMOVE n.age";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Properties().add("age", 40l, PropertyType.LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties removed: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(1);
        assertThat(graph.getNodeById(1).get().getAllProperties().size()).isEqualTo(1);
        assertThat((Long) graph.getNodeById(1).get().getAllProperties().getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.age > 25 REMOVE n.age");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }
}
