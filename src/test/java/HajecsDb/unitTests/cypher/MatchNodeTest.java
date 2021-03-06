package HajecsDb.unitTests.cypher;

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
import static org.hajecsdb.graphs.core.PropertyType.INT;
import static org.hajecsdb.graphs.core.PropertyType.STRING;
import static org.hajecsdb.graphs.cypher.clauses.helpers.ContentType.NODE;

@RunWith(MockitoJUnitRunner.class)
public class MatchNodeTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void matchAnyNodeInEmptyGraphExpectedEmptyListTest() {

        // given
        String command = "MATCH (n)";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n)");
        assertThat(result.getResults()).isEmpty();
    }

    @Test
    public void matchNodeByLabelInEmptyGraphExpectedEmptyListTest() {

        // given
        String command = "MATCH (n: Person)";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person)");
        assertThat(result.getResults()).isEmpty();
    }

    @Test
    public void matchNodeByLabelInFilledGraphExpectedOneNodeTest() {

        // given
        String command = "MATCH (n: Person)";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Person"), null);


        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(NODE);
        expectedResultRow.setNode(transactionalGraphService.context(transaction).getNodeById(1).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person)");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0)).isEqualTo(expectedResultRow);
    }

    @Test
    public void matchNodeByLabelInFilledGraphExpectedListWithAllNodesTest() {

        // given
        String command = "MATCH (n: Person)";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "second", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "third", STRING));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(1l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(2l).get());

        ResultRow expectedResultRow3 = new ResultRow();
        expectedResultRow3.setContentType(NODE);
        expectedResultRow3.setNode(transactionalGraphService.context(transaction).getNodeById(3l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Person)");
        assertThat(result.getResults()).hasSize(3);
        assertThat(result.getResults().get(0)).isEqualTo(expectedResultRow1);
        assertThat(result.getResults().get(1)).isEqualTo(expectedResultRow2);
        assertThat(result.getResults().get(2)).isEqualTo(expectedResultRow3);
    }

    @Test
    public void matchAnyNodeExpectedListWithAllNodesTest() {

        // given
        String command = "MATCH (n)";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "second", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "third", STRING));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(1l).get());

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(2l).get());

        ResultRow expectedResultRow3 = new ResultRow();
        expectedResultRow3.setContentType(NODE);
        expectedResultRow3.setNode(transactionalGraphService.context(transaction).getNodeById(3l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n)");
        assertThat(result.getResults()).hasSize(3);
        assertThat(result.getResults().get(0)).isEqualTo(expectedResultRow1);
        assertThat(result.getResults().get(1)).isEqualTo(expectedResultRow2);
        assertThat(result.getResults().get(2)).isEqualTo(expectedResultRow3);
    }

    @Test
    public void doubleMatchTest() {

        // given
        String command = "MATCH (n) MATCH (n: Person) WHERE n.name = 'first'";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "second", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "third", STRING));

        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(NODE);
        expectedResultRow1.setNode(transactionalGraphService.context(transaction).getNodeById(1l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) MATCH (n: Person) WHERE n.name = 'first'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0)).isEqualTo(expectedResultRow1);
    }

    @Test
    public void matchBySingleParameterExpectedOneNodeTest() {

        // given
        String command = "MATCH (n:Person {name: 'Adam'})";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "Adam", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "third", STRING));

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(2l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0)).isEqualTo(expectedResultRow2);
    }

    @Test
    public void matchByTwoParametersExpectedOneNodeTest() {

        // given
        String command = "MATCH (n:Person {name: 'Adam', age: 25})";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "first", STRING));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "Adam", STRING).add("age", 25, INT));
        transactionalGraphService.context(transaction).createNode(new Label("Person"), new Properties().add("name", "third", STRING));

        ResultRow expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(NODE);
        expectedResultRow2.setNode(transactionalGraphService.context(transaction).getNodeById(2l).get());

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);

        //then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0)).isEqualTo(expectedResultRow2);
    }
}
