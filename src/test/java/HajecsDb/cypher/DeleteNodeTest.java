package HajecsDb.cypher;

import HajecsDb.utils.NodeComparator;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class DeleteNodeTest {

    private Graph graph;
    private CypherExecutor cypherExecutor = new CypherExecutor();
    private NodeComparator nodeComparator = new NodeComparator();


    @Test
    public void DeleteAllNodesWhenGraphIsEmptyTest() {
        // given
        String command = "MATCH (n) DELETE n";
        graph = new GraphImpl("pathDir", "graphDir");

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 0");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void DeleteAllNodesWhenGraphHasOneNodeTest() {
        // given
        String command = "MATCH (n) DELETE n";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void DeleteAllNodesWhenGraphHasTreeNodesTest() {
        // given
        String command = "MATCH (n) DELETE n";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode();
        graph.createNode();
        graph.createNode();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 3");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void DeleteNodesWithUselessLabelWhenGraphIsEmptyTest() {
        // given
        String command = "MATCH (n: Useless) DELETE n";
        graph = new GraphImpl("pathDir", "graphDir");

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 0");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Useless) DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void DeleteNodesWithUselessLabelWhenGraphHasOneNodeTest() {
        // given
        String command = "MATCH (n: Useless) DELETE n";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Useless"));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Useless) DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void DeleteNodesWithUselessLabelWhenGraphHasTreeNodesTest() {
        // given
        String command = "MATCH (n: Useless) DELETE n";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Useless"));
        graph.createNode(new Label("Useless"));
        graph.createNode(new Label("Useless"));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 3");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Useless) DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void DeleteNodesWithAndrewNameTest() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Andrew' DELETE n";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Useless"), new Properties().add("name", "Andrew", STRING));
        graph.createNode(new Label("Useless"), new Properties().add("name", "Victor", STRING));
        graph.createNode(new Label("Useless"), new Properties().add("name", "Amelia", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(2);
        assertThat(graph.getNodeById(1).isPresent()).isFalse();
        assertThat(graph.getNodeById(2).isPresent()).isTrue();
        assertThat(graph.getNodeById(3).isPresent()).isTrue();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andrew' DELETE n");
        assertThat(result.getResults()).hasSize(1);

        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo("Nodes deleted: 1");
    }

    @Test
    public void DeleteNodesWithAgeGreaterThan25Test() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Andrew' DELETE n";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Useless"), new Properties().add("name", "Andrew", STRING).add("age", 25l, LONG));
        graph.createNode(new Label("Useless"), new Properties().add("name", "Victor", STRING).add("age", 21l, LONG));
        graph.createNode(new Label("Useless"), new Properties().add("name", "Amelia", STRING).add("age", 30l, LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(2);
        assertThat(graph.getNodeById(1).isPresent()).isFalse();
        assertThat(graph.getNodeById(2).isPresent()).isTrue();
        assertThat(graph.getNodeById(3).isPresent()).isTrue();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andrew' DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

//    @Test
//    public void DeleteNodesWhereAgeIsBetween25An30Test() {
//        // given
//        String command = "MATCH (n) WHERE n.age > 25 AND n.age < 30 DELETE n";
//        graph = new GraphImpl("pathDir", "graphDir");
//        graph.createNode(new Label("Useless"), new Properties().add("name", "Andrew", STRING).add("age", 25l, LONG));
//        graph.createNode(new Label("Useless"), new Properties().add("name", "Victor", STRING).add("age", 21l, LONG));
//        graph.createNode(new Label("Useless"), new Properties().add("name", "Amelia", STRING).add("age", 30l, LONG));
//        cypherExecutor = new CypherExecutor(graph);
//
//        ResultRow expectedResultRow = new ResultRow();
//        expectedResultRow.setContentType(ContentType.STRING);
//        expectedResultRow.setMessage("Nodes deleted: 2");
//
//        // when
//        Result result = cypherExecutor.execute(command);
//
//        //then
//        assertThat(graph.getAllNodes()).hasSize(1);
//        assertThat(graph.getNodeById(1).isPresent()).isFalse();
//        assertThat(graph.getNodeById(2).isPresent()).isTrue();
//        assertThat(graph.getNodeById(3).isPresent()).isFalse();
//        assertThat(result.isCompleted()).isTrue();
//        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andrew' DELETE n");
//        assertThat(result.getResults()).hasSize(1);
//        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
//        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
//    }
}
