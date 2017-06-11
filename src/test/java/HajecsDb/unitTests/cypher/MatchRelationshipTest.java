package HajecsDb.unitTests.cypher;

import org.hajecsdb.graphs.core.Label;
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
public class MatchRelationshipTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();

    ResultRow expectedResultRow1;
    ResultRow expectedResultRow2;
    ResultRow expectedResultRow3;
    ResultRow expectedResultRow4;
    ResultRow expectedResultRow5;
    ResultRow expectedResultRow6;
    ResultRow expectedResultRow7;

    {
        Transaction transaction = transactionManager.createTransaction();
        cypherExecutor.execute(transaction, "CREATE (p: Vampire {name: 'Selene'})");
        cypherExecutor.execute(transaction, "CREATE (p: Vampire {name: 'Victor'})");
        cypherExecutor.execute(transaction, "CREATE (p: Hibrid {name: 'Marcus'})");
        cypherExecutor.execute(transaction, "CREATE (p: Hibrid {name: 'Michael'})");
        cypherExecutor.execute(transaction, "CREATE (p: Vampire {name: 'Kraven'})");
        cypherExecutor.execute(transaction, "CREATE (p: Vampire {name: 'Tanis'})");
        cypherExecutor.execute(transaction, "CREATE (p: Lykan {name: 'William'})");
        cypherExecutor.execute(transaction, "MATCH (m {name: 'Marcus'}) MATCH (s {name: 'Selene'}) CREATE (m)-[p:KNOW]->(s)");
        cypherExecutor.execute(transaction, "MATCH (m {name: 'Selene'}) MATCH (s {name: 'Tanis'}) CREATE (m)-[p:KNOW]->(s)");
        cypherExecutor.execute(transaction, "MATCH (v {name: 'Victor'}) MATCH (s {name: 'Selene'}) CREATE (v)-[p:LIKES]->(s)");
        cypherExecutor.execute(transaction, "MATCH (v {name: 'Victor'}) MATCH (s {name: 'Kraven'}) CREATE (v)-[p:LIKES]->(s)");

        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(ContentType.NODE);
        expectedResultRow1.setNode(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1l).get());

        expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(ContentType.NODE);
        expectedResultRow2.setNode(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2l).get());

        expectedResultRow3 = new ResultRow();
        expectedResultRow3.setContentType(ContentType.NODE);
        expectedResultRow3.setNode(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(3l).get());

        expectedResultRow4 = new ResultRow();
        expectedResultRow4.setContentType(ContentType.NODE);
        expectedResultRow4.setNode(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(4l).get());

        expectedResultRow5 = new ResultRow();
        expectedResultRow5.setContentType(ContentType.NODE);
        expectedResultRow5.setNode(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(5l).get());

        expectedResultRow6 = new ResultRow();
        expectedResultRow6.setContentType(ContentType.NODE);
        expectedResultRow6.setNode(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(6l).get());

        expectedResultRow7 = new ResultRow();
        expectedResultRow7.setContentType(ContentType.NODE);
        expectedResultRow7.setNode(cypherExecutor.getTransactionalGraphService().getPersistentNodeById(7l).get());

        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(7);
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentRelationships()).hasSize(4);
        assertThat(cypherExecutor.getTransactionalGraphService().findPersistentRelationship(3, 1, new Label("KNOW")).isPresent()).isTrue();
        assertThat(cypherExecutor.getTransactionalGraphService().findPersistentRelationship(1, 6, new Label("KNOW")).isPresent()).isTrue();
        assertThat(cypherExecutor.getTransactionalGraphService().findPersistentRelationship(2, 1, new Label("LIKES")).isPresent()).isTrue();
        assertThat(cypherExecutor.getTransactionalGraphService().findPersistentRelationship(2, 5, new Label("LIKES")).isPresent()).isTrue();
    }

    @Test
    public void matchWithoutRegardToDirectionTest() {

        // given
        String command = "MATCH (vampire: Vampire)--(other { name: 'Selene' })";
        Transaction transaction = transactionManager.createTransaction();
        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (vampire: Vampire)--(other { name: 'Selene' })");
        assertThat(result.getResults()).hasSize(3);
        assertThat(result.getResults().values()).contains(expectedResultRow2, expectedResultRow3, expectedResultRow6);
    }

    @Test
    public void matchWithoutRegardToTypeOrDirectionTest() {
        // given
        String command = "MATCH (vampire { name: 'Victor' })--(other)";
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (vampire { name: 'Victor' })--(other)");
        assertThat(result.getResults()).hasSize(2);
        assertThat(result.getResults().values()).contains(expectedResultRow1, expectedResultRow5);
    }

    @Test
    public void matchByLabelWithoutRegardToDirectionTest() {
        // given
        String command1 = "MATCH (n: Hibrid { name: 'Marcus' })--(v: Vampire)";
        String command2 = "MATCH (n: Vampire { name: 'Selene' })--(v: Vampire)";
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command1);
        Result result2 = cypherExecutor.execute(transaction, command2);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();


        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n: Hibrid { name: 'Marcus' })--(v: Vampire)");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0)).isEqualTo(expectedResultRow1);

        assertThat(result2.isCompleted()).isTrue();
        assertThat(result2.getCommand()).isEqualTo("MATCH (n: Vampire { name: 'Selene' })--(v: Vampire)");
        assertThat(result2.getResults()).hasSize(3);
        assertThat(result2.getResults().values()).contains(expectedResultRow2, expectedResultRow3, expectedResultRow6);
    }

    @Test
    public void matchByDirectedRelationshipAndVariableTest() {
        // given
        String command = "MATCH (n:Vampire { name: 'Selene' })-[r]->(other)";
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n:Vampire { name: 'Selene' })-[r]->(other)");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().values()).contains(expectedResultRow6);
    }

    @Test
    public void matchByRelationshipTypeTest() {
        // given
        String command = "MATCH (n:Vampire { name: 'Selene' })<-[r:LIKES]-(other)";
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transaction, command);

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n:Vampire { name: 'Selene' })<-[r:LIKES]-(other)");
        assertThat(result.getResults()).hasSize(2);
        assertThat(result.getResults().values()).contains(expectedResultRow2, expectedResultRow3);
    }
}
