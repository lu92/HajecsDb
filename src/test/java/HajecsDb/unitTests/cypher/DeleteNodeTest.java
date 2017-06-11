package HajecsDb.unitTests.cypher;

import HajecsDb.unitTests.utils.NodeComparator;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class DeleteNodeTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private NodeComparator nodeComparator = new NodeComparator();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void DeleteAllNodesWhenGraphIsEmptyTest() {
        // given
        String command = "MATCH (n) DELETE n";
        Transaction transaction = transactionManager.createTransaction();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 0");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).isEmpty();
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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).isEmpty();
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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), null);
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), null);
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 3");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).isEmpty();
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
        Transaction transaction = transactionManager.createTransaction();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 0");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Useless) DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    //

    @Test
    public void DeleteNodesWithUselessLabelWhenGraphHasOneNodeTest() {
        // given
        String command = "MATCH (n: Useless) DELETE n";
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).isEmpty();
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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), null);
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), null);
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 3");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).isEmpty();
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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Andrew", STRING));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Victor", STRING));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Amelia", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(2);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).isPresent()).isFalse();
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2).isPresent()).isTrue();
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(3).isPresent()).isTrue();
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
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Andrew", STRING).add("age", 25l, LONG));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Victor", STRING).add("age", 21l, LONG));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Amelia", STRING).add("age", 30l, LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(2);
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).isPresent()).isFalse();
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2).isPresent()).isTrue();
        assertThat(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(3).isPresent()).isTrue();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andrew' DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }
}
