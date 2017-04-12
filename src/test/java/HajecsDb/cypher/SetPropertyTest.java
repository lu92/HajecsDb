package HajecsDb.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.cypher.ContentType;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class SetPropertyTest {

    private Graph graph;
    private CypherExecutor cypherExecutor = new CypherExecutor();

    @Test
    public void updateNamePropertyOfEmptyGraph() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'";
        graph = new GraphImpl("pathDir", "graphDir");

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 0");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void updateNamePropertyOfSingleNode() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Properties().add("name", "Selene", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(1);

        Node fetchedNode = graph.getNodeById(1).get();
        assertThat(fetchedNode.getAllProperties().size()).isEqualTo(2);
        assertThat((Long) fetchedNode.getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat((String) fetchedNode.getProperty("name").get().getValue()).isEqualTo("Kate");
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void updateNamePropertyOfTwoFromThreeNodes() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Properties().add("name", "Selene", STRING));
        graph.createNode(new Properties().add("name", "Selene", STRING));
        graph.createNode(new Properties().add("name", "Amelia", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 2");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(3);

        Node fetchedNode1 = graph.getNodeById(1).get();
        assertThat(fetchedNode1.getAllProperties().size()).isEqualTo(2);
        assertThat((Long) fetchedNode1.getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat((String) fetchedNode1.getProperty("name").get().getValue()).isEqualTo("Kate");

        Node fetchedNode2 = graph.getNodeById(2).get();
        assertThat(fetchedNode2.getAllProperties().size()).isEqualTo(2);
        assertThat((Long) fetchedNode2.getProperty("id").get().getValue()).isEqualTo(2l);
        assertThat((String) fetchedNode2.getProperty("name").get().getValue()).isEqualTo("Kate");

        Node fetchedNode3 = graph.getNodeById(3).get();
        assertThat(fetchedNode3.getAllProperties().size()).isEqualTo(2);
        assertThat((Long) fetchedNode3.getProperty("id").get().getValue()).isEqualTo(3l);
        assertThat((String) fetchedNode3.getProperty("name").get().getValue()).isEqualTo("Amelia");

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void setLastnamePropertyOnEmptyGraph() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Andres' SET n.lastname = 'Taylor'";
        graph = new GraphImpl("pathDir", "graphDir");

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 0");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andres' SET n.lastname = 'Taylor'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void setLastnamePropertyOnSingleNode() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Andres' SET n.lastname = 'Taylor'";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Properties().add("name", "Andres", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 1");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(1);


        Node fetchedNode = graph.getNodeById(1).get();
        assertThat(fetchedNode.getAllProperties().size()).isEqualTo(3);
        assertThat((Long) fetchedNode.getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat((String) fetchedNode.getProperty("name").get().getValue()).isEqualTo("Andres");
        assertThat((String) fetchedNode.getProperty("lastname").get().getValue()).isEqualTo("Taylor");
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andres' SET n.lastname = 'Taylor'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void setLastnamePropertyOnThreeNode() {
        // given
        String command = "MATCH (n) SET n.lastname = 'Taylor'";
        graph = new GraphImpl("pathDir", "graphDir");
        graph.createNode(new Properties().add("name", "Andres", STRING));
        graph.createNode(new Properties().add("age", 25l, LONG));
        graph.createNode(new Properties().add("university", "UJ", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 3");

        // when
        Result result = cypherExecutor.execute(graph, command);

        //then
        assertThat(graph.getAllNodes()).hasSize(3);

        Node fetchedNode1 = graph.getNodeById(1).get();
        assertThat(fetchedNode1.getAllProperties().size()).isEqualTo(3);
        assertThat((Long) fetchedNode1.getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat((String) fetchedNode1.getProperty("name").get().getValue()).isEqualTo("Andres");
        assertThat((String) fetchedNode1.getProperty("lastname").get().getValue()).isEqualTo("Taylor");

        Node fetchedNode2 = graph.getNodeById(2).get();
        assertThat(fetchedNode2.getAllProperties().size()).isEqualTo(3);
        assertThat((Long) fetchedNode2.getProperty("id").get().getValue()).isEqualTo(2l);
        assertThat((Long) fetchedNode2.getProperty("age").get().getValue()).isEqualTo(25l);
        assertThat((String) fetchedNode2.getProperty("lastname").get().getValue()).isEqualTo("Taylor");

        Node fetchedNode3 = graph.getNodeById(3).get();
        assertThat(fetchedNode3.getAllProperties().size()).isEqualTo(3);
        assertThat((Long) fetchedNode3.getProperty("id").get().getValue()).isEqualTo(3l);
        assertThat((String) fetchedNode3.getProperty("university").get().getValue()).isEqualTo("UJ");
        assertThat((String) fetchedNode3.getProperty("lastname").get().getValue()).isEqualTo("Taylor");

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) SET n.lastname = 'Taylor'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }
}
