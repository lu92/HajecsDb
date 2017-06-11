package HajecsDb.unitTests.transactions;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.lockMechanism.EntityLockRecognizer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityLockRecognizerTest {

    private EntityLockRecognizer entityLockRecognizer = new EntityLockRecognizer();
    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void Test() {
    }

    public EntityLockRecognizerTest() {
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
    }

//    @Test
//    public void qualifyNodesByLabelInEmptyGraphTest() {
//        // given
//        String query = "MATCH (n: Vampire) SET n.likeBlood = 'yes'";
//        Transaction transaction = transactionManager.createTransaction();
//
//        // when
//        List<Entity> entities = entityLockRecognizer.determineEntities(transactionalGraphService, transaction, query);
//
//        // then
//        assertThat(entities).isEmpty();
//    }
//
//    @Test
//    public void qualifyNodesByLabelTest() {
//        // given
//        String query = "MATCH (n: Vampire) SET n.likeBlood = 'yes'";
//
//        // when
//        List<Entity> entities = entityLockRecognizer.determineEntities(graph, query);
//
//        // then
//        assertThat(entities).hasSize(4);
//        assertThat(entities).containsExactly(graph.getNodeById(1).get(), graph.getNodeById(2).get(),
//                graph.getNodeById(5).get(), graph.getNodeById(6).get());
//    }
}
