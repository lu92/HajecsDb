package HajecsDb.unitTests.transactions;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.restLayer.Session;
import org.hajecsdb.graphs.restLayer.SessionPool;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.lockMechanism.EntityLockRecognizer;
import org.hajecsdb.graphs.transactions.lockMechanism.LockingProtocolManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class LockingProtocolManagerTest {

    private EntityLockRecognizer entityLockRecognizer = new EntityLockRecognizer();
    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();
    private SessionPool sessionPool = new SessionPool();
    private LockingProtocolManager lockingProtocolManager = new LockingProtocolManager();
    private Graph graph;

    public LockingProtocolManagerTest() {
        graph = new GraphImpl("pathDir", "graphDir");
        cypherExecutor.execute(graph, "CREATE (p: Vampire {name: 'Selene'})");
        cypherExecutor.execute(graph, "CREATE (p: Vampire {name: 'Victor'})");
        cypherExecutor.execute(graph, "CREATE (p: Hibrid {name: 'Marcus'})");
        cypherExecutor.execute(graph, "CREATE (p: Hibrid {name: 'Michael'})");
        cypherExecutor.execute(graph, "CREATE (p: Vampire {name: 'Kraven'})");
        cypherExecutor.execute(graph, "CREATE (p: Vampire {name: 'Tanis'})");
        cypherExecutor.execute(graph, "CREATE (p: Lykan {name: 'William'})");
        cypherExecutor.execute(graph, "MATCH (m {name: 'Marcus'}) MATCH (s {name: 'Selene'}) CREATE (m)-[p:KNOW]->(s)");
        cypherExecutor.execute(graph, "MATCH (m {name: 'Selene'}) MATCH (s {name: 'Tanis'}) CREATE (m)-[p:KNOW]->(s)");
        cypherExecutor.execute(graph, "MATCH (v {name: 'Victor'}) MATCH (s {name: 'Selene'}) CREATE (v)-[p:LIKES]->(s)");
        cypherExecutor.execute(graph, "MATCH (v {name: 'Victor'}) MATCH (s {name: 'Kraven'}) CREATE (v)-[p:LIKES]->(s)");
    }

    @Test
    public void testToMove() {
        // given

        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        session.setCypherExecutor(cypherExecutor);

        String query = "MATCH (n: Vampire) SET n.likeBlood = 'yes'";

        // when
        Transaction transaction = session.beginTransaction();
        session.performQuery(graph, query);

//        transaction.commit();

        lockingProtocolManager.growingPhase(graph, transaction);
        transaction.getScope().getOperations()
                .forEach(operation -> cypherExecutor.execute(graph, operation.getCypherQuery()));
        transaction.commit();
        lockingProtocolManager.shrinkingPhase();

        assertThat(graph.getNodeById(1).get().hasProperty("likeBlood")).isTrue();
        assertThat(graph.getNodeById(2).get().hasProperty("likeBlood")).isTrue();
        assertThat(graph.getNodeById(3).get().hasProperty("likeBlood")).isFalse();
        assertThat(graph.getNodeById(4).get().hasProperty("likeBlood")).isFalse();
        assertThat(graph.getNodeById(5).get().hasProperty("likeBlood")).isTrue();
        assertThat(graph.getNodeById(6).get().hasProperty("likeBlood")).isTrue();
        assertThat(graph.getNodeById(7).get().hasProperty("likeBlood")).isFalse();
    }
}
