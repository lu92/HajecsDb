package HajecsDb.unitTests.transactions;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.lockMechanism.EntityLockRecognizer;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityLockRecognizerTest {

    private EntityLockRecognizer entityLockRecognizer = new EntityLockRecognizer();
    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();
    private TransactionalGraphService transactionalGraphService;

    @Test
    public void Test() {
    }

    public EntityLockRecognizerTest() {
        transactionalGraphService = new TransactionalGraphService();
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
