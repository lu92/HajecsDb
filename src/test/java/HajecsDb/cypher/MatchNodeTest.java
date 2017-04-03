package HajecsDb.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.STRING;
import static org.hajecsdb.graphs.cypher.ContentType.NODE;

@RunWith(MockitoJUnitRunner.class)
public class MatchNodeTest {

    private Graph graph;
    private CypherExecutor cypherExecutor;

    @Test
    public void matchAnyNodeInEmptyGraphExpectedEmptyListTest() {

        // given
        String command = "MATCH (n)";
        graph = new GraphImpl("pathDir", "graphDir");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n)");
        assertThat(result.getResults()).isEmpty();
    }

    @Test
    public void matchNodeByLabelInEmptyGraphExpectedEmptyListTest() {

        // given
        String command = "MATCH (n: Person)";
        graph = new GraphImpl("pathDir", "graphDir");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person)");
        assertThat(result.getResults()).isEmpty();
    }

    @Test
    public void matchNodeByLabelInFilledGraphExpectedOneNodeTest() {

        // given
        String command = "MATCH (n: Person)";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Person"));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(NODE);
        expectedResultRow.setNode(graph.getNodeById(1l).get());

        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person)");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(1)).isEqualTo(expectedResultRow);
    }

    @Test
    public void matchNodeByLabelInFilledGraphExpectedListWithAllNodesTest() {

        // given
        String command = "MATCH (n: Person)";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        graph.createNode(new Label("Person"), new Properties().add("name", "second", STRING));
        graph.createNode(new Label("Person"), new Properties().add("name", "third", STRING));
        cypherExecutor = new CypherExecutor(graph);

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(graph.getNodeById(1l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(graph.getNodeById(2l).get());

        ResultRow expectedResultRow3 = new ResultRow();
        expectedResultRow3.setContentType(NODE);
        expectedResultRow3.setNode(graph.getNodeById(3l).get());

        // when
        Result result = cypherExecutor.execute(command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person)");
        assertThat(result.getResults()).hasSize(3);
        assertThat(result.getResults().get(1)).isEqualTo(expectedResultRow1);
        assertThat(result.getResults().get(2)).isEqualTo(expectedResultRow2);
        assertThat(result.getResults().get(3)).isEqualTo(expectedResultRow3);
    }

    @Test
    public void matchAnyNodeExpectedListWithAllNodesTest() {

        // given
        String command = "MATCH (n)";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        graph.createNode(new Label("Person"), new Properties().add("name", "second", STRING));
        graph.createNode(new Label("Person"), new Properties().add("name", "third", STRING));
        cypherExecutor = new CypherExecutor(graph);

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(graph.getNodeById(1l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(graph.getNodeById(2l).get());

        ResultRow expectedResultRow3 = new ResultRow();
        expectedResultRow3.setContentType(NODE);
        expectedResultRow3.setNode(graph.getNodeById(3l).get());

        // when
        Result result = cypherExecutor.execute(command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n)");
        assertThat(result.getResults()).hasSize(3);
        assertThat(result.getResults().get(1)).isEqualTo(expectedResultRow1);
        assertThat(result.getResults().get(2)).isEqualTo(expectedResultRow2);
        assertThat(result.getResults().get(3)).isEqualTo(expectedResultRow3);
    }

    @Test
    public void doubleMatchTest() {

        // given
        String command = "MATCH (n) MATCH (n: Person) WHERE n.name = 'first'";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        graph.createNode(new Label("Person"), new Properties().add("name", "second", STRING));
        graph.createNode(new Label("Person"), new Properties().add("name", "third", STRING));
        cypherExecutor = new CypherExecutor(graph);

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(graph.getNodeById(1l).get());

        // when
        Result result = cypherExecutor.execute(command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) MATCH (n: Person) WHERE n.name = 'first'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0)).isEqualTo(expectedResultRow1);
    }
}
