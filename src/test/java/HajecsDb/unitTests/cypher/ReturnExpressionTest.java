package HajecsDb.unitTests.cypher;

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

@RunWith(MockitoJUnitRunner.class)
public class ReturnExpressionTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void expectedIntValueFromNode() {
        // given
        String command = "CREATE (n: Person {age: 25}) RETURN n.age";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

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
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        // then
        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.NODE);
        expectedResultRow.setNode(transactionalGraphService.getPersistentNodeById(1).get());

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow);
    }

    @Test
    public void expectedRelationshipValue() {
        // given
        String command = "MATCH (f {name : 'first'}) MATCH (s {name : 'second'}) CREATE (f)-[r:CONNECTED]->(s) RETURN r";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name: 'first'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name: 'second'})");

        //when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        // then
        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(ContentType.RELATIONSHIP);
        expectedResultRow1.setRelationship(transactionalGraphService.getPersistentRelationshipById(3).get());

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow1);
    }

}
