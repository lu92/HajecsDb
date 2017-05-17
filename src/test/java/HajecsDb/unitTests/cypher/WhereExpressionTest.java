package HajecsDb.unitTests.cypher;

import HajecsDb.unitTests.utils.NodeComparator;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.*;
import static org.hajecsdb.graphs.cypher.clauses.helpers.ContentType.NODE;

@RunWith(MockitoJUnitRunner.class)
public class WhereExpressionTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private NodeComparator nodeComparator = new NodeComparator();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void matchByAgeInEmptyDbTest() {

        // given
        String command = "MATCH (n) WHERE n.age = 25";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.age = 25");
        assertThat(result.getResults()).isEmpty();
    }

    @Test
    public void matchByNameExpectedOneNodeTest() {

        // given
        String command = "MATCH (n) WHERE n.name = 'first'";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "second", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "third", STRING));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(1).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'first'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(nodeComparator.isSame(result.getResults().get(0).getNode(), expectedResultRow1.getNode()));
    }

    @Test
    public void matchByAgeExpectedTwoNodesTest() {

        // given
        String command = "MATCH (n) WHERE n.age = 25";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 30l, LONG));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(1l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(2l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.age = 25");
        assertThat(result.getResults()).hasSize(2);

        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow1.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(0).getNode(), expectedResultRow1.getNode()));

        assertThat(result.getResults().get(1).getContentType()).isEqualTo(expectedResultRow2.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(1).getNode(), expectedResultRow2.getNode())).isTrue();
    }

    @Test
    public void matchByAgeGreaterThan25ExpectedTwoNodesTest() {

        // given
        String command = "MATCH (n) WHERE n.age > 25";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 26l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 26l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25l, LONG));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(1l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(2l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.age > 25");
        assertThat(result.getResults()).hasSize(2);

        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow1.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(0).getNode(), expectedResultRow1.getNode()));

        assertThat(result.getResults().get(1).getContentType()).isEqualTo(expectedResultRow2.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(1).getNode(), expectedResultRow2.getNode())).isTrue();
    }

    @Test
    public void matchByAgeGreaterThanOrEqual25ExpectedTwoNodesTest() {

        // given
        String command = "MATCH (n) WHERE n.age >= 25";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 26l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 24l, LONG));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(1l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(2l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();


        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.age >= 25");
        assertThat(result.getResults()).hasSize(2);

        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow1.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(0).getNode(), expectedResultRow1.getNode()));

        assertThat(result.getResults().get(1).getContentType()).isEqualTo(expectedResultRow2.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(1).getNode(), expectedResultRow2.getNode())).isTrue();
    }

    @Test
    public void matchByAgeBetween25And30ExpectedTwoNodesTest() {

        // given
        String command = "MATCH (n) WHERE n.age >= 25 AND n.age <= 30";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 24l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 30l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 31l, LONG));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(2l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(3l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.age >= 25 AND n.age <= 30");
        assertThat(result.getResults()).hasSize(2);

        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow1.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(0).getNode(), expectedResultRow1.getNode()));

        assertThat(result.getResults().get(1).getContentType()).isEqualTo(expectedResultRow2.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(1).getNode(), expectedResultRow2.getNode())).isTrue();
    }

    @Test
    public void matchByNameExpectedNodesWithAmeliaOrVictorNamesTest() {

        // given
        String command = "MATCH (n) WHERE n.name = 'Victor' OR n.name = 'Amelia'";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "Amelia", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "Henry", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 30l, LONG));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "Victor", STRING));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(1l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(4l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Victor' OR n.name = 'Amelia'");
        assertThat(result.getResults()).hasSize(2);

        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow1.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(0).getNode(), expectedResultRow1.getNode()));

        assertThat(result.getResults().get(1).getContentType()).isEqualTo(expectedResultRow2.getContentType());
        assertThat(nodeComparator.isSame(result.getResults().get(1).getNode(), expectedResultRow2.getNode())).isTrue();
    }

    @Test
    public void matchByIdFunctionExpectedOneNodeTest() {

        // given
        String command = "MATCH (n) WHERE id(n) = 3";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25, INT));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25, INT));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 30, INT));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(3l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE id(n) = 3");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(1)).isEqualTo(expectedResultRow1);
    }

    @Test
    public void matchByLabelAndIdFunctionExpectedOneNodeTest() {

        // given
        String command = "MATCH (n: Person) WHERE id(n) = 3";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25, INT));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 25, INT));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("age", 30, INT));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(3l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person) WHERE id(n) = 3");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(1)).isEqualTo(expectedResultRow1);
    }
}
