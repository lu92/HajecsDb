package HajecsDb.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ReturnExpressionTest {

    private Graph graph;
    private CypherExecutor cypherExecutor;

    @Test
    public void expectedIntValueFromNode() {
        // given
        String command = "CREATE (n: Person {age: 25}) RETURN n.age";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.INT);
        expectedResultRow.setIntValue(25);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow);
    }

    @Test
    public void expectedStringValueFromNode() {
        // given
        String command = "CREATE (n: Person {name: 'Robert'}) RETURN n.name";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Robert");

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow);
    }

    @Test
    public void expectedDoubleValueFromNode() {
        // given
        String command = "CREATE (n: Person {salary: 3000.00}) RETURN n.salary";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.DOUBLE);
        expectedResultRow.setDoubleValue(3000.00);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow);
    }

    @Test
    public void expectedNullValueFromNode() {
        // given
        String command = "CREATE (n: Person {age: 25}) RETURN n.missingProperty";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(ContentType.NONE);
        assertThat(result.getResults().get(0).getNode()).isNull();
    }

    @Test
    public void expectedNodeValue() {
        // given
        String command = "CREATE (n: Person {age: 25}) RETURN n";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        // when
        Result result = cypherExecutor.execute(command);

        // then
        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.NODE);
        expectedResultRow.setNode(graph.getNodeById(1).get());

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow);
    }

    @Test
    public void expectedRelationshipValue() {
        // given
        String command = "MATCH (f {name : 'first'}) MATCH (s {name : 'second'}) CREATE (f)-[r:CONNECTED]->(s) RETURN r";
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);
        cypherExecutor.execute("CREATE (n: Person {name: 'first'})");
        cypherExecutor.execute("CREATE (n: Person {name: 'second'})");

        //when
        Result result = cypherExecutor.execute(command);

        // then
        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(ContentType.RELATIONSHIP);
        expectedResultRow1.setRelationship(graph.getRelationshipById(3).get());

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow1);
    }

}
