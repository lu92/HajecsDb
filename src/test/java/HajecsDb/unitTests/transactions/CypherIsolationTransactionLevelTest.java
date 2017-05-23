package HajecsDb.unitTests.transactions;

import net.jodah.concurrentunit.Waiter;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.TimeoutException;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class CypherIsolationTransactionLevelTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionalGraphService transactionalGraphService;
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void createTwoSameNodesInSeparateTransactionsTest() throws TimeoutException {

        // given
        transactionalGraphService = new TransactionalGraphService();

        Waiter waiter = new Waiter();


        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran1 = transactionManager.createTransaction();
                Result result1 = cypherExecutor.execute(transactionalGraphService, tran1,
                        "CREATE (n: Person {name : 'David'}) RETURN n");
                transactionalGraphService.context(tran1).commit();
                waiter.resume();
            }
        });


        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran2 = transactionManager.createTransaction();
                Result result2 = cypherExecutor.execute(transactionalGraphService, tran2,
                        "CREATE (n: Person {name : 'David'}) RETURN n");
                transactionalGraphService.context(tran2).commit();
                waiter.resume();
            }
        });

        // when

        user1.start();
        waiter.await(100);
        user2.start();
        waiter.await(100);

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(2);

        Node fetchedNode1 = transactionalGraphService.getPersistentNodeById(1).get();
        assertThat(fetchedNode1.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "David"));

        Node fetchedNode2 = transactionalGraphService.getPersistentNodeById(2).get();
        assertThat(fetchedNode2.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 2l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "David"));
    }

    @Test
    public void addNodeToEachTransactionScopeTest() throws TimeoutException {

        // given
        transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name : 'David'}) RETURN n");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name : 'Walter'}) RETURN n");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name : 'Adam'}) RETURN n");
        transactionalGraphService.context(transaction).commit();

        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(3);

        Waiter waiter = new Waiter();

        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran1 = transactionManager.createTransaction();
                Result result1 = cypherExecutor.execute(transactionalGraphService, tran1, "CREATE (n: Person {name : 'Selene'}) RETURN n");
                Result nodesInScopeOfTran1 = cypherExecutor.execute(transactionalGraphService, tran1, "MATCH (n: Person) RETURN n");
                transactionalGraphService.context(tran1).commit();
                waiter.resume();
            }
        });

        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran2 = transactionManager.createTransaction();
                Result result2 = cypherExecutor.execute(transactionalGraphService, tran2, "CREATE (n: Person {name : 'Victor'}) RETURN n");
                Result nodesInScopeOfTran2 = cypherExecutor.execute(transactionalGraphService, tran2, "MATCH (n: Person) RETURN n");
                transactionalGraphService.context(tran2).commit();
                waiter.resume();
            }
        });


        // when
        user1.start();
        waiter.await(100);
        user2.start();
        waiter.await(100);


        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(5);

        Node fetchedNode4 = transactionalGraphService.getPersistentNodeById(4).get();
        assertThat(fetchedNode4.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 4l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "Selene"));

        Node fetchedNode5 = transactionalGraphService.getPersistentNodeById(5).get();
        assertThat(fetchedNode5.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 5l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "Victor"));
    }

    @Test
    public void addRelationshipToEachTransactionScopeTest() throws TimeoutException {
        // given
        transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name : 'David'}) RETURN n");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name : 'Walter'}) RETURN n");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name : 'Adam'}) RETURN n");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name : 'Victor'}) RETURN n");
        transactionalGraphService.context(transaction).commit();

        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(4);

        Waiter waiter = new Waiter();

        // when

        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran1 = transactionManager.createTransaction();
                cypherExecutor.execute(transactionalGraphService, tran1,
                        "MATCH (david: Person {name : 'David'}) MATCH (walter: Person {name : 'Walter'}) CREATE (david)-[p:IS_BROTHER]->(walter) RETURN p");
                Result davidResultNode = cypherExecutor.execute(transactionalGraphService, tran1, "MATCH (n: Person {name: 'David'}) RETURN n");
                Result walterResultNode = cypherExecutor.execute(transactionalGraphService, tran1, "MATCH (n: Person {name: 'Walter'}) RETURN n");

                assertThat(davidResultNode.getResults().get(0).getNode().getRelationships()).hasSize(1);
                assertThat(walterResultNode.getResults().get(0).getNode().getRelationships()).hasSize(1);
                transactionalGraphService.context(tran1).commit();
                waiter.resume();
            }
        });


        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran2 = transactionManager.createTransaction();
                cypherExecutor.execute(transactionalGraphService, tran2,
                        "MATCH (victor: Person {name : 'Victor'}) MATCH (adam: Person {name : 'Adam'}) CREATE (victor)-[p:KNOWS]->(adam) RETURN p");
                Result adamResultNode = cypherExecutor.execute(transactionalGraphService, tran2, "MATCH (n: Person {name: 'Adam'}) RETURN n");
                Result victorResultNode = cypherExecutor.execute(transactionalGraphService, tran2, "MATCH (n: Person {name: 'Victor'}) RETURN n");

                assertThat(adamResultNode.getResults().get(0).getNode().getRelationships()).hasSize(1);
                assertThat(victorResultNode.getResults().get(0).getNode().getRelationships()).hasSize(1);
                transactionalGraphService.context(tran2).commit();
                waiter.resume();
            }
        });

        // validate committed graph - should not contain relatioships
        assertThat(transactionalGraphService.getAllPersistentRelationships()).hasSize(0);

        user1.start();
        waiter.await(100);
        user2.start();
        waiter.await(100);

        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(4);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).hasSize(2);
    }

    @Test
    public void twoTransactionsTryModifySameNodeExpectedTimeOutBecauseOfAnyTransactionCommitsItsWorkTest() throws TimeoutException {

        // given
        transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (n: Person {name : 'David'}) RETURN n");
        transactionalGraphService.context(transaction).commit();


        Transaction tran1 = transactionManager.createTransaction();
        Transaction tran2 = transactionManager.createTransaction();

        Waiter waiter = new Waiter();

        // when

        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Result result1 = cypherExecutor.execute(transactionalGraphService, tran1,
                        "MATCH (n: Person) WHERE n.name = 'David' SET n.name = 'David1'");

                System.out.println("user1 modified node!");

                assertThat(transactionalGraphService.context(tran1).getNodeById(1).get().getProperty("name").get())
                        .isEqualTo(new Property("name", STRING, "David1"));

                waiter.resume();

            }
        });

        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Result result2 = cypherExecutor.execute(transactionalGraphService, tran2,
                        "MATCH (n: Person) WHERE n.name = 'David' SET n.name = 'David2'");

                System.out.println("user2 modified node!");

                assertThat(transactionalGraphService.context(tran2).getNodeById(2).get().getProperty("name").get())
                        .isEqualTo(new Property("name", STRING, "David2"));

                waiter.resume();

            }
        });

        try {
            user1.start();
            waiter.await(100);

            user2.start();
            waiter.await(100);

        } catch (TimeoutException e) {
            // then
            assertThat(e.getMessage()).isEqualTo("Test timed out while waiting for an expected result");
        }

        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);

        Node fetchedNode1 = transactionalGraphService.getPersistentNodeById(1).get();
        assertThat(fetchedNode1.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "David"));
    }

}
