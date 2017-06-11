package HajecsDb.unitTests.cypher;

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

@RunWith(MockitoJUnitRunner.class)
public class ReturnExpressionTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void expectedIntValueFromNode() {
        // given
        String command = "CREATE (n: Person {age: 25}) RETURN n.age";
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

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
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

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
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

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
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

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
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        // then
        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.NODE);
        expectedResultRow.setNode(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get());

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow);
    }

    @Test
    public void expectedRelationshipValue() {
        // given
        String command = "MATCH (f {name : 'first'}) MATCH (s {name : 'second'}) CREATE (f)-[r:CONNECTED]->(s) RETURN r";
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.execute(transaction, "CREATE (n: Person {name: 'first'})");
        cypherExecutor.execute(transaction, "CREATE (n: Person {name: 'second'})");

        //when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        // then
        ResultRow expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(ContentType.RELATIONSHIP);
        expectedResultRow1.setRelationship(cypherExecutor.getTransactionalGraphService().getPersistentRelationshipById(3).get());

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).containsOnly(expectedResultRow1);
    }

}
