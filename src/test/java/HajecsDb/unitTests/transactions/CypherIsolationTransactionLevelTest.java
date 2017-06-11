package HajecsDb.unitTests.transactions;

import net.jodah.concurrentunit.Waiter;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
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
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void createTwoSameNodesInSeparateTransactionsTest() throws TimeoutException {

        // given
        Waiter waiter = new Waiter();


        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran1 = transactionManager.createTransaction();
                Result result1 = cypherExecutor.execute(tran1,
                        "CREATE (n: Person {name : 'David'}) RETURN n");
                cypherExecutor.getTransactionalGraphService().context(tran1).commit();
                waiter.resume();
            }
        });


        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran2 = transactionManager.createTransaction();
                Result result2 = cypherExecutor.execute(tran2,
                        "CREATE (n: Person {name : 'David'}) RETURN n");
                cypherExecutor.getTransactionalGraphService().context(tran2).commit();
                waiter.resume();
            }
        });

        // when

        user1.start();
        waiter.await(100);
        user2.start();
        waiter.await(100);

        // then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(2);

        Node fetchedNode1 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get();
        assertThat(fetchedNode1.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "David"));

        Node fetchedNode2 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2).get();
        assertThat(fetchedNode2.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 2l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "David"));
    }

    @Test
    public void addNodeToEachTransactionScopeTest() throws TimeoutException {

        // given
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.execute(transaction, "CREATE (n: Person {name : 'David'}) RETURN n");
        cypherExecutor.execute(transaction, "CREATE (n: Person {name : 'Walter'}) RETURN n");
        cypherExecutor.execute(transaction, "CREATE (n: Person {name : 'Adam'}) RETURN n");
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(3);

        Waiter waiter = new Waiter();

        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran1 = transactionManager.createTransaction();
                Result result1 = cypherExecutor.execute(tran1, "CREATE (n: Person {name : 'Selene'}) RETURN n");
                Result nodesInScopeOfTran1 = cypherExecutor.execute(tran1, "MATCH (n: Person) RETURN n");
                cypherExecutor.getTransactionalGraphService().context(tran1).commit();
                waiter.resume();
            }
        });

        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran2 = transactionManager.createTransaction();
                Result result2 = cypherExecutor.execute(tran2, "CREATE (n: Person {name : 'Victor'}) RETURN n");
                Result nodesInScopeOfTran2 = cypherExecutor.execute(tran2, "MATCH (n: Person) RETURN n");
                cypherExecutor.getTransactionalGraphService().context(tran2).commit();
                waiter.resume();
            }
        });


        // when
        user1.start();
        waiter.await(100);
        user2.start();
        waiter.await(100);


        // then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(5);

        Node fetchedNode4 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(4).get();
        assertThat(fetchedNode4.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 4l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "Selene"));

        Node fetchedNode5 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(5).get();
        assertThat(fetchedNode5.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 5l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "Victor"));
    }

    @Test
    public void addRelationshipToEachTransactionScopeTest() throws TimeoutException {
        // given
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.execute(transaction, "CREATE (n: Person {name : 'David'}) RETURN n");
        cypherExecutor.execute(transaction, "CREATE (n: Person {name : 'Walter'}) RETURN n");
        cypherExecutor.execute(transaction, "CREATE (n: Person {name : 'Adam'}) RETURN n");
        cypherExecutor.execute(transaction, "CREATE (n: Person {name : 'Victor'}) RETURN n");
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(4);

        Waiter waiter = new Waiter();

        // when

        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran1 = transactionManager.createTransaction();
                cypherExecutor.execute(tran1,
                        "MATCH (david: Person {name : 'David'}) MATCH (walter: Person {name : 'Walter'}) CREATE (david)-[p:IS_BROTHER]->(walter) RETURN p");
                Result davidResultNode = cypherExecutor.execute(tran1, "MATCH (n: Person {name: 'David'}) RETURN n");
                Result walterResultNode = cypherExecutor.execute(tran1, "MATCH (n: Person {name: 'Walter'}) RETURN n");

                assertThat(davidResultNode.getResults().get(0).getNode().getRelationships()).hasSize(1);
                assertThat(walterResultNode.getResults().get(0).getNode().getRelationships()).hasSize(1);
                cypherExecutor.getTransactionalGraphService().context(tran1).commit();
                waiter.resume();
            }
        });


        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran2 = transactionManager.createTransaction();
                cypherExecutor.execute(tran2,
                        "MATCH (victor: Person {name : 'Victor'}) MATCH (adam: Person {name : 'Adam'}) CREATE (victor)-[p:KNOWS]->(adam) RETURN p");
                Result adamResultNode = cypherExecutor.execute(tran2, "MATCH (n: Person {name: 'Adam'}) RETURN n");
                Result victorResultNode = cypherExecutor.execute(tran2, "MATCH (n: Person {name: 'Victor'}) RETURN n");

                assertThat(adamResultNode.getResults().get(0).getNode().getRelationships()).hasSize(1);
                assertThat(victorResultNode.getResults().get(0).getNode().getRelationships()).hasSize(1);
                cypherExecutor.getTransactionalGraphService().context(tran2).commit();
                waiter.resume();
            }
        });

        // validate committed graph - should not contain relatioships
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentRelationships()).hasSize(0);

        user1.start();
        waiter.await(100);
        user2.start();
        waiter.await(100);

        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(4);
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentRelationships()).hasSize(2);
    }

    @Test
    public void twoTransactionsTryModifySameNodeExpectedTimeOutBecauseOfAnyTransactionCommitsItsWorkTest() throws TimeoutException {

        // given
        Transaction transaction = transactionManager.createTransaction();
        cypherExecutor.execute(transaction, "CREATE (n: Person {name : 'David'}) RETURN n");
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();


        Transaction tran1 = transactionManager.createTransaction();
        Transaction tran2 = transactionManager.createTransaction();

        Waiter waiter = new Waiter();

        // when

        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Result result1 = cypherExecutor.execute(tran1,
                        "MATCH (n: Person) WHERE n.name = 'David' SET n.name = 'David1'");

                System.out.println("user1 modified node!");

                assertThat(cypherExecutor.getTransactionalGraphService().context(tran1).getNodeById(1).get().getProperty("name").get())
                        .isEqualTo(new Property("name", STRING, "David1"));

                waiter.resume();

            }
        });

        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Result result2 = cypherExecutor.execute(tran2,
                        "MATCH (n: Person) WHERE n.name = 'David' SET n.name = 'David2'");

                System.out.println("user2 modified node!");

                assertThat(cypherExecutor.getTransactionalGraphService().context(tran2).getNodeById(2).get().getProperty("name").get())
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

        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(1);

        Node fetchedNode1 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get();
        assertThat(fetchedNode1.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "David"));
    }

}
