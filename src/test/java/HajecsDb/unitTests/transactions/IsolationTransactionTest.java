package HajecsDb.unitTests.transactions;

import HajecsDb.unitTests.utils.NodeComparator;
import net.jodah.concurrentunit.Waiter;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.restLayer.Session;
import org.hajecsdb.graphs.restLayer.SessionPool;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.DOUBLE;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class IsolationTransactionTest {

    private static TransactionManager transactionManager = new TransactionManager();
    private static SessionPool sessionPool = new SessionPool();
    private NodeComparator nodeComparator = new NodeComparator();


    @Test
    public void updateSamePropertyOfNodeInTwoTransactionsThenRollbackTest() throws InterruptedException, TimeoutException {


        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // this transaction prepares state of graph
        Transaction transaction = session.beginTransaction();
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties()
                        .add(new Property("name", STRING, "Alice"))
                        .add(new Property("age", LONG, 25l)));

        transactionalGraphService.context(transaction).commit();

        // validate content of graph
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        assertThat(transactionalGraphService.isEntityLocked(alice)).isFalse();

        Waiter waiter = new Waiter();


        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {

                Transaction tran1 = session.beginTransaction();
                System.out.println(Thread.currentThread().getName() + " performing tran1");

                // tran1 sets age of alice to 20
                transactionalGraphService.context(tran1).setPropertyToNode(1, new Property("age", LONG, 20l));

                // validate age of alice in tran1
                waiter.assertEquals(transactionalGraphService.context(tran1).getNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 20l));

                // validate age of alice in committed graph
                waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 25l));

                transactionalGraphService.context(tran1).rollback();
                System.out.println(Thread.currentThread().getName() + " rollbacked tran1");

                waiter.resume();
            }
        });

        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {

                Transaction tran2 = session.beginTransaction();
                System.out.println(Thread.currentThread().getName() + " performing tran2");

                // tran2 sets age of alice to 30
                transactionalGraphService.context(tran2).setPropertyToNode(1, new Property("age", LONG, 30l));

                // validate age of alice in tran1
                waiter.assertEquals(transactionalGraphService.context(tran2).getNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 30l));

                // validate age of alice in committed graph
                waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 25l));

                transactionalGraphService.context(tran2).rollback();
                waiter.resume();
            }
        });


        user1.start();
        waiter.await(1000);

        Thread.sleep(100);

        user2.start();
        waiter.await(1000);


        // validate age of alice in committed graph
        assertThat(transactionalGraphService.getPersistentNodeById(1).get().getProperty("age").get())
                .isEqualTo(new Property("age", LONG, 25l));
    }

    @Test
    public void twoTransactionsTryAddNewDifferentNodesSecondTransactionAterFirstFinishedTest() throws TimeoutException, InterruptedException {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // for now graph should be empty - nothing was committed
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();


        Waiter waiter = new Waiter();

        final List<Node> createdNodes = new ArrayList<>();

        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {

                final Transaction tran1 = session.beginTransaction();
                System.out.println(Thread.currentThread().getName() + " performing tran1");

                // transaction 1
                Node alice = transactionalGraphService.context(tran1)
                        .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
                Node gina = transactionalGraphService.context(tran1)
                        .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

                createdNodes.add(alice);
                createdNodes.add(gina);

                // check if nodes from tran2 are visible in tran1 - should NOT
                assertThat(transactionalGraphService.context(tran1).getAllNodes()).containsOnly(alice, gina);

                // after commitment tran1, transactionalGraphService should contain alice and gina's nodes
                transactionalGraphService.context(tran1).commit();
                System.out.println(Thread.currentThread().getName() + " tran1 committed");

                assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(2);
                assertThat(transactionalGraphService.isEntityLocked(alice)).isFalse();
                assertThat(transactionalGraphService.isEntityLocked(gina)).isFalse();

                waiter.resume();
            }
        });

        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {

                Transaction tran2 = session.beginTransaction();
                System.out.println(Thread.currentThread().getName() + " performing tran2");

                // transaction 2
                Node bob = transactionalGraphService.context(tran2)
                        .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
                Node frank = transactionalGraphService.context(tran2)
                        .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Frank")));

                createdNodes.add(bob);
                createdNodes.add(frank);

                // after commitment tran2, transactionalGraphService should contain alice gina bob and frank's nodes
                transactionalGraphService.context(tran2).commit();
                System.out.println(Thread.currentThread().getName() + " tran2 committed");

                waiter.resume();
            }
        });

        // when
        user1.start();
        waiter.await(1000);

        Thread.sleep(20);

        user2.start();
        waiter.await(1000);


        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(4);

        transactionalGraphService.getAllPersistentNodes().forEach(searchedNode ->
        {
            Node equivalentNode = createdNodes.stream().
                    filter(node -> node.getProperty("name").get().equals(searchedNode.getProperty("name").get()))
                    .findFirst().get();
            assertThat(nodeComparator.isSame(searchedNode, equivalentNode)).isTrue();
        });
    }


    @Test
    public void updateSamePropertyOfNodeSecondTransactionShouldBeBlocked() throws InterruptedException, TimeoutException {

        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // this transaction prepares state of graph
        Transaction transaction = session.beginTransaction();
        transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties()
                        .add(new Property("name", STRING, "Alice"))
                        .add(new Property("age", LONG, 25l)));

        transactionalGraphService.context(transaction).commit();

        // validate content of graph
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);

        Waiter waiter = new Waiter();


        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {

                System.out.println("Thread " + Thread.currentThread().getName() + " performing tran1");
                Transaction tran1 = session.beginTransaction();

                // tran1 sets age of alice to 20
                transactionalGraphService.context(tran1).setPropertyToNode(1, new Property("age", LONG, 20l));
                waiter.assertEquals(transactionalGraphService.context(tran1).getNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 20l));

                // validate age of alice in tran1
                waiter.assertEquals(transactionalGraphService.context(tran1).getNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 20l));

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // validate age of alice in committed graph
                waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 25l));

                waiter.resume();
            }
        });

        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran2 = session.beginTransaction();
                System.out.println("Thread " + Thread.currentThread().getName() + " performing tran2");

                // tran2 sets age of alice to 30
                transactionalGraphService.context(tran2).setPropertyToNode(1, new Property("age", LONG, 30l));
                waiter.assertEquals(transactionalGraphService.context(tran2).getNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 30l));

                waiter.assertEquals(transactionalGraphService.context(tran2).getNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 30l));

                // validate age of alice in committed graph
                waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("age").get(), new Property("age", LONG, 25l));

                waiter.resume();
            }
        });


        user1.start();
        waiter.await(1000);
        Thread.sleep(10);

        // when
        user2.start();
        try {
            waiter.await(100);
        } catch (TimeoutException e) {
            // then
            assertThat(e.getMessage()).isEqualTo("Test timed out while waiting for an expected result");
        }

    }


    // concurentupdateOfSameNode_second should wait until forist one will commit

    @Test
    public void twoTransactionsTryToUpdateSameNodeSecondTransactionShouldBeBlockedAndReleasedAfterFirstCommitTest() throws TimeoutException, InterruptedException {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // this transaction prepares state of graph
        Transaction transaction = session.beginTransaction();
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties()
                        .add(new Property("name", STRING, "Alice"))
                        .add(new Property("age", LONG, 25l))
                        .add(new Property("salary", DOUBLE, 3000.00)));

        transactionalGraphService.context(transaction).commit();

        // validate content of graph
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);

        Waiter waiter = new Waiter();


        Thread user1 = new Thread(new Runnable() {
            @Override
            public void run() {

                System.out.println("Thread " + Thread.currentThread().getName() + " performing tran1");
                Transaction tran1 = session.beginTransaction();

                // tran1 decreases alice's salaray of 100
                double salary = (Double) transactionalGraphService.context(tran1).getNodeById(alice.getId()).get().getProperty("salary").get().getValue();
                salary -= 100;
                transactionalGraphService.context(tran1).setPropertyToNode(1, new Property("salary", DOUBLE, salary));


                // validate salary of alice in tran1
                waiter.assertEquals(transactionalGraphService.context(tran1).getNodeById(1).get().getProperty("salary").get(), new Property("salary", DOUBLE, 2900.00));

                // validate age of alice in committed graph
                waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("salary").get(), new Property("salary", DOUBLE, 3000.00));

                try {
                    Thread.sleep(50);  // to simulate user's work
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                transactionalGraphService.context(tran1).commit();

                // validate age of alice after commit
                waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("salary").get(), new Property("salary", DOUBLE, 2900.00));

                waiter.resume();
            }
        });

        Thread user2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction tran2 = session.beginTransaction();
                System.out.println("Thread " + Thread.currentThread().getName() + " performing tran2");

                // tran2 decreases alice's salaray of 200
                double salary = (Double) transactionalGraphService.context(tran2).getNodeById(alice.getId()).get().getProperty("salary").get().getValue();
                salary -= 200;
                transactionalGraphService.context(tran2).setPropertyToNode(1, new Property("salary", DOUBLE, salary));
                waiter.assertEquals(transactionalGraphService.context(tran2).getNodeById(1).get().getProperty("salary").get(), new Property("salary", DOUBLE, 2700.00));

                // validate age of alice in committed graph
                waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("salary").get(), new Property("salary", DOUBLE, 2900.00));

                transactionalGraphService.context(tran2).commit();

                // validate age of alice after commit
                waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("salary").get(), new Property("salary", DOUBLE, 2700.00));

                waiter.resume();
            }
        });


        user1.start();
        waiter.await(100);

        // first user should start his work
        Thread.sleep(5);

        // when
        user2.start();
        waiter.await(100);

        // validate committed node
        waiter.assertEquals(transactionalGraphService.getPersistentNodeById(1).get().getProperty("salary").get(), new Property("salary", DOUBLE, 2700.00));
    }

}
