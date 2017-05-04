package HajecsDb.unitTests.transactions;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.restLayer.Session;
import org.hajecsdb.graphs.restLayer.SessionPool;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class VisibilityOfTransactionScopeTest {

    private TransactionManager transactionManager = new TransactionManager();
    private SessionPool sessionPool = new SessionPool();

    @Test
    public void visibilityOfTwoSeparatedTransactionsTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction 1
        Transaction tran1 = session.beginTransaction();
        Node alice = transactionalGraphService.context(tran1)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(tran1)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        // transaction 2
        Transaction tran2 = session.beginTransaction();
        Node bob = transactionalGraphService.context(tran2)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Node frank = transactionalGraphService.context(tran2)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Frank")));

        // for now graph should be empty - nothing was committed
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();

        // check if nodes from tran2 are visible in tran1 - should NOT
        assertThat(transactionalGraphService.context(tran1).getAllNodes()).containsOnly(alice, gina);

        // check if nodes from tran1 are visible in tran2 - should NOT
        assertThat(transactionalGraphService.context(tran2).getAllNodes()).containsOnly(bob, frank);

        // after commitment tran1, transactionalGraphService should contain alice and gina's nodes
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getAllPersistentNodes()).containsOnly(alice, gina);

        // after commitment tran2, transactionalGraphService should contain alice gina bob and frank's nodes
        transactionalGraphService.context(tran2).commit();
        assertThat(transactionalGraphService.getAllPersistentNodes()).containsOnly(alice, gina, bob, frank);
    }

    @Test
    public void visibilityOfSharedNodeInTwoTransactionsTest() {
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

        // tran1 sets age of alice to 20
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).setProperty(1, new Property("age", LONG, 20l));
        assertThat(transactionalGraphService.context(tran1).getNodeById(1).get().getProperty("age").get())
                .isEqualTo(new Property("age", LONG, 20l));

        // tran1 sets age of alice to 30
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).setProperty(1, new Property("age", LONG, 30l));
        assertThat(transactionalGraphService.context(tran2).getNodeById(1).get().getProperty("age").get())
                .isEqualTo(new Property("age", LONG, 30l));


        // validate age of alice in committed graph
        assertThat(transactionalGraphService.getPersistentNodeById(1).get().getProperty("age").get())
                .isEqualTo(new Property("age", LONG, 25l));

        // validate age of alice in tran1
        assertThat(transactionalGraphService.context(tran1).getNodeById(1).get().getProperty("age").get())
                .isEqualTo(new Property("age", LONG, 20l));

        // validate age of alice in tran2
        assertThat(transactionalGraphService.context(tran2).getNodeById(1).get().getProperty("age").get())
                .isEqualTo(new Property("age", LONG, 30l));

        transactionalGraphService.context(tran1).rollback();
        transactionalGraphService.context(tran2).rollback();

        // validate age of alice in committed graph
        assertThat(transactionalGraphService.getPersistentNodeById(1).get().getProperty("age").get())
                .isEqualTo(new Property("age", LONG, 25l));

    }

}
