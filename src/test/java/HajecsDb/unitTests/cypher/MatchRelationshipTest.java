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
public class MatchRelationshipTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();
    private TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

    ResultRow expectedResultRow1;
    ResultRow expectedResultRow2;
    ResultRow expectedResultRow3;
    ResultRow expectedResultRow4;
    ResultRow expectedResultRow5;
    ResultRow expectedResultRow6;
    ResultRow expectedResultRow7;

    {
        Transaction transaction = transactionManager.createTransaction();
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (p: Vampire {name: 'Selene'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (p: Vampire {name: 'Victor'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (p: Hibrid {name: 'Marcus'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (p: Hibrid {name: 'Michael'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (p: Vampire {name: 'Kraven'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (p: Vampire {name: 'Tanis'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (p: Lykan {name: 'William'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "MATCH (m {name: 'Marcus'}) MATCH (s {name: 'Selene'}) CREATE (m)-[p:KNOW]->(s)");
        cypherExecutor.execute(transactionalGraphService, transaction, "MATCH (m {name: 'Selene'}) MATCH (s {name: 'Tanis'}) CREATE (m)-[p:KNOW]->(s)");
        cypherExecutor.execute(transactionalGraphService, transaction, "MATCH (v {name: 'Victor'}) MATCH (s {name: 'Selene'}) CREATE (v)-[p:LIKES]->(s)");
        cypherExecutor.execute(transactionalGraphService, transaction, "MATCH (v {name: 'Victor'}) MATCH (s {name: 'Kraven'}) CREATE (v)-[p:LIKES]->(s)");

        transactionalGraphService.context(transaction).commit();

        expectedResultRow1 = new ResultRow();
        expectedResultRow1.setContentType(ContentType.NODE);
        expectedResultRow1.setNode(transactionalGraphService.getPersistentNodeById(1l).get());

        expectedResultRow2 = new ResultRow();
        expectedResultRow2.setContentType(ContentType.NODE);
        expectedResultRow2.setNode(transactionalGraphService.getPersistentNodeById(2l).get());

        expectedResultRow3 = new ResultRow();
        expectedResultRow3.setContentType(ContentType.NODE);
        expectedResultRow3.setNode(transactionalGraphService.getPersistentNodeById(3l).get());

        expectedResultRow4 = new ResultRow();
        expectedResultRow4.setContentType(ContentType.NODE);
        expectedResultRow4.setNode(transactionalGraphService.getPersistentNodeById(4l).get());

        expectedResultRow5 = new ResultRow();
        expectedResultRow5.setContentType(ContentType.NODE);
        expectedResultRow5.setNode(transactionalGraphService.getPersistentNodeById(5l).get());

        expectedResultRow6 = new ResultRow();
        expectedResultRow6.setContentType(ContentType.NODE);
        expectedResultRow6.setNode(transactionalGraphService.getPersistentNodeById(6l).get());

        expectedResultRow7 = new ResultRow();
        expectedResultRow7.setContentType(ContentType.NODE);
        expectedResultRow7.setNode(transactionalGraphService.getPersistentNodeById(7l).get());

        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(7);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).hasSize(4);
//        assertThat(graph.findRelationship(3, 1, new Label("KNOW"))).isNotNull();
//        assertThat(graph.findRelationship(1, 6, new Label("KNOW"))).isNotNull();
//        assertThat(graph.findRelationship(2, 1, new Label("LIKES"))).isNotNull();
//        assertThat(graph.findRelationship(2, 5, new Label("LIKES"))).isNotNull();
    }

    @Test
    public void matchWithoutRegardToDirectionTest() {

        // given
        String command = "MATCH (vampire: Vampire)--(other { name: 'Selene' })";
        Transaction transaction = transactionManager.createTransaction();
        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

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
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

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
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command1);
        Result result2 = cypherExecutor.execute(transactionalGraphService, transaction, command2);
        transactionalGraphService.context(transaction).commit();


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
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

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
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n:Vampire { name: 'Selene' })<-[r:LIKES]-(other)");
        assertThat(result.getResults()).hasSize(2);
        assertThat(result.getResults().values()).contains(expectedResultRow2, expectedResultRow3);
    }
}
