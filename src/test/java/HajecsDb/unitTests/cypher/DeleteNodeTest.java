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
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 0");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label(""), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label(""), null);
        transactionalGraphService.context(transaction).createNode(new Label(""), null);
        transactionalGraphService.context(transaction).createNode(new Label(""), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 3");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 0");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Useless"), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Useless"), null);
        transactionalGraphService.context(transaction).createNode(new Label("Useless"), null);
        transactionalGraphService.context(transaction).createNode(new Label("Useless"), null);

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 3");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Andrew", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Victor", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Amelia", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(2);
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isFalse();
        assertThat(transactionalGraphService.getPersistentNodeById(2).isPresent()).isTrue();
        assertThat(transactionalGraphService.getPersistentNodeById(3).isPresent()).isTrue();
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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Andrew", STRING).add("age", 25l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Victor", STRING).add("age", 21l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Useless"), new Properties().add("name", "Amelia", STRING).add("age", 30l, LONG));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Nodes deleted: 1");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(2);
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isFalse();
        assertThat(transactionalGraphService.getPersistentNodeById(2).isPresent()).isTrue();
        assertThat(transactionalGraphService.getPersistentNodeById(3).isPresent()).isTrue();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andrew' DELETE n");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }
}
