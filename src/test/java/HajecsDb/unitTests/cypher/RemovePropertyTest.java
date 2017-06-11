package HajecsDb.unitTests.cypher;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class RemovePropertyTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void removeSinglePropertyFromNodeTest() {
        // given
        String command = "MATCH (n) REMOVE n.age";
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("age", 40l, LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties removed: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(1);
        Optional<Node> persistentNode = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1);
        assertThat(persistentNode.isPresent()).isTrue();
        assertThat(persistentNode.get().getAllProperties().size()).isEqualTo(2);
        assertThat(persistentNode.get().getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(persistentNode.get().getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, ""));
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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("age", 40l, LONG));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("age", 30l, LONG));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("age", 20l, LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties removed: 3");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(3);

        Optional<Node> persistentNode1 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1);
        assertThat(persistentNode1.get().getAllProperties().size()).isEqualTo(2);
        assertThat(persistentNode1.get().getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(persistentNode1.get().getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, ""));


        Optional<Node> persistentNode2 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2);
        assertThat(persistentNode2.get().getAllProperties().size()).isEqualTo(2);
        assertThat(persistentNode2.get().getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 2l));
        assertThat(persistentNode2.get().getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, ""));

        Optional<Node> persistentNode3 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(3);
        assertThat(persistentNode3.get().getAllProperties().size()).isEqualTo(2);
        assertThat(persistentNode3.get().getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 3l));
        assertThat(persistentNode3.get().getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, ""));

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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Person"), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Labels removed: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(1);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get().getAllProperties().size()).isEqualTo(1);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get().getAllProperties().getProperty("label").isPresent()).isFalse();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) REMOVE n:Person");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }
//
    @Test
    public void removeLabelFromThreeNodesTest() {
        // given
        String command = "MATCH (n: Person) REMOVE n:Person";
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Person"), null);
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Person"), null);
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Person"), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Labels removed: 3");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(3);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get().getAllProperties().size()).isEqualTo(1);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get().getAllProperties().getProperty("label").isPresent()).isFalse();
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2).get().getAllProperties().size()).isEqualTo(1);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2).get().getAllProperties().getProperty("label").isPresent()).isFalse();
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(3).get().getAllProperties().size()).isEqualTo(1);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(3).get().getAllProperties().getProperty("label").isPresent()).isFalse();
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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties removed: 0");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(1);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get().getAllProperties().size()).isEqualTo(2);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get().getAllProperties().getProperty("fakeProperty").isPresent()).isFalse();
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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("age", 40l, PropertyType.LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties removed: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(1);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get().getAllProperties().size()).isEqualTo(2);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get().getAllProperties().getProperty("id").get().getValue()).isEqualTo(1l);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.age > 25 REMOVE n.age");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }
}
