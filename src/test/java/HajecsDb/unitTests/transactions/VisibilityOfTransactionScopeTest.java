package HajecsDb.unitTests.transactions;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.restLayer.Session;
import org.hajecsdb.graphs.restLayer.SessionPool;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

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

    @Ignore
    public void visibilityOfSharedNodeInTwoTransactionsTest() throws InterruptedException {
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
        transactionalGraphService.context(tran1).setPropertyToNode(1, new Property("age", LONG, 20l));
        assertThat(transactionalGraphService.context(tran1).getNodeById(1).get().getProperty("age").get())
                .isEqualTo(new Property("age", LONG, 20l));

        // tran2 sets age of alice to 30
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).setPropertyToNode(1, new Property("age", LONG, 30l));
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

    @Test
    public void visibilityOfSharedRelationshipInTwoTransactionsTest() {
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

        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties()
                        .add(new Property("name", STRING, "Gina"))
                        .add(new Property("age", LONG, 27l)));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties()
                        .add(new Property("name", STRING, "Bob"))
                        .add(new Property("age", LONG, 24l)));

        Relationship alice_knows_gina = transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        Relationship alice_likes_bob = transactionalGraphService.context(transaction).createRelationship(alice.getId(), bob.getId(), new Label("LIKES"));
        Relationship gina_hates_bob = transactionalGraphService.context(transaction).createRelationship(gina.getId(), bob.getId(), new Label("HATES"));

        transactionalGraphService.context(transaction).commit();

        // validate content of graph
        assertThat(transactionalGraphService.getAllPersistentNodes()).containsOnly(alice, gina, bob);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).containsOnly(alice_knows_gina, alice_likes_bob, gina_hates_bob);


        assertThat(transactionalGraphService.getPersistentNodeById(alice.getId()).get().getRelationships()).containsOnly(alice_knows_gina, alice_likes_bob);
        assertThat(transactionalGraphService.getPersistentNodeById(gina.getId()).get().getRelationships()).containsOnly(gina_hates_bob, alice_knows_gina);
        assertThat(transactionalGraphService.getPersistentNodeById(bob.getId()).get().getRelationships()).containsOnly(gina_hates_bob, alice_likes_bob);


        // changes of tran1
        Transaction tran1 = session.beginTransaction();
        Node ed = transactionalGraphService.context(tran1)
                .createNode(new Label("Person"), new Properties()
                        .add(new Property("name", STRING, "ED"))
                        .add(new Property("age", LONG, 30l)));

        Relationship ed_knows_bob = transactionalGraphService.context(tran1).createRelationship(ed.getId(), bob.getId(), new Label("KNOWS"));
        transactionalGraphService.context(tran1).setPropertyToNode(ed.getId(), new Property("lastname", STRING, "Smith"));
        assertThat(transactionalGraphService.context(tran1).getNodeById(ed.getId()).get().hasProperty("lastname")).isTrue();
        Relationship bob_likes_alice = transactionalGraphService.context(tran1).createRelationship(bob.getId(), alice.getId(), new Label("LIKES"));
        assertThat(transactionalGraphService.context(tran1).getNodeById(bob.getId()).get().getRelationships())
                .containsOnly(alice_likes_bob, gina_hates_bob, bob_likes_alice, ed_knows_bob);

        transactionalGraphService.context(tran1).setPropertyToNode(gina.getId(), new Property("born", LONG, 1990));
        assertThat(transactionalGraphService.context(tran1).getNodeById(gina.getId()).get().getProperty("born").get())
                .isEqualTo(new Property("born", LONG, 1990));

        transactionalGraphService.context(tran1).deleteRelationship(alice_knows_gina.getId());
        assertThat(transactionalGraphService.context(tran1).getNodeById(alice.getId()).get().getRelationships()).containsOnly(alice_likes_bob, bob_likes_alice);
        assertThat(transactionalGraphService.context(tran1).getNodeById(gina.getId()).get().getRelationships()).containsOnly(gina_hates_bob);

        transactionalGraphService.context(tran1).deleteRelationship(gina_hates_bob.getId());
        assertThat(transactionalGraphService.context(tran1).getNodeById(gina.getId()).get().getRelationships()).isEmpty();
        assertThat(transactionalGraphService.context(tran1).getNodeById(bob.getId()).get().getRelationships()).containsOnly(alice_likes_bob, bob_likes_alice, ed_knows_bob);

        transactionalGraphService.context(tran1).deletePropertyFromNode(gina.getId(), "born");
        assertThat(transactionalGraphService.context(tran1).getNodeById(gina.getId()).get().hasProperty("born")).isFalse();

        transactionalGraphService.context(tran1).deleteNode(gina.getId());
        assertThat(transactionalGraphService.context(tran1).getNodeById(gina.getId()).isPresent()).isFalse();


        // check if changes of tran1 violates persistent graph - should no
        assertThat(transactionalGraphService.getAllPersistentNodes()).containsOnly(alice, gina, bob);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).containsOnly(alice_knows_gina, alice_likes_bob, gina_hates_bob);
        assertThat(transactionalGraphService.getPersistentNodeById(alice.getId()).get().getRelationships()).containsOnly(alice_knows_gina, alice_likes_bob);
        assertThat(transactionalGraphService.getPersistentNodeById(gina.getId()).get().getRelationships()).containsOnly(gina_hates_bob, alice_knows_gina);
        assertThat(transactionalGraphService.getPersistentNodeById(bob.getId()).get().getRelationships()).containsOnly(gina_hates_bob, alice_likes_bob);

        // changes of tran 2
        Transaction tran2 = session.beginTransaction();
        assertThat(transactionalGraphService.context(tran2).getNodeById(ed.getId()).isPresent()).isFalse();
        assertThat(transactionalGraphService.context(tran2).getNodeById(gina.getId()).isPresent()).isTrue();
        Node frank = transactionalGraphService.context(tran2).createNode(new Label("Person"), new Properties()
                .add(new Property("name", STRING, "Frank"))
                .add(new Property("age", LONG, 26l)));

        assertThat(transactionalGraphService.context(tran2).getNodeById(frank.getId()).isPresent()).isTrue();
        Relationship frank_loves_gina = transactionalGraphService.context(tran2).createRelationship(frank.getId(), gina.getId(), new Label("LOVES"));
        assertThat(transactionalGraphService.context(tran2).getNodeById(gina.getId()).get().getRelationships()).containsOnly(frank_loves_gina, gina_hates_bob, alice_knows_gina);


        // check if changes of tran2 violates persistent graph - should no
        assertThat(transactionalGraphService.getAllPersistentNodes()).containsOnly(alice, gina, bob);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).containsOnly(alice_knows_gina, alice_likes_bob, gina_hates_bob);
        assertThat(transactionalGraphService.getPersistentNodeById(alice.getId()).get().getRelationships()).containsOnly(alice_knows_gina, alice_likes_bob);
        assertThat(transactionalGraphService.getPersistentNodeById(gina.getId()).get().getRelationships()).containsOnly(gina_hates_bob, alice_knows_gina);
        assertThat(transactionalGraphService.getPersistentNodeById(bob.getId()).get().getRelationships()).containsOnly(gina_hates_bob, alice_likes_bob);

        Node updated_Ed = transactionalGraphService.context(tran1).getNodeById(ed.getId()).get();

        // commit tran1
        transactionalGraphService.context(tran1).commit();

        // validate changes of tran1 after commit
        assertThat(transactionalGraphService.getAllPersistentNodes()).containsOnly(alice, bob, updated_Ed);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).containsOnly(alice_likes_bob, bob_likes_alice, ed_knows_bob);
        assertThat(transactionalGraphService.getPersistentNodeById(alice.getId()).get().getRelationships()).containsOnly(alice_likes_bob, bob_likes_alice);
        assertThat(transactionalGraphService.getPersistentNodeById(bob.getId()).get().getRelationships()).containsOnly(bob_likes_alice, alice_likes_bob, ed_knows_bob);
        assertThat(transactionalGraphService.getPersistentNodeById(ed.getId()).get().getRelationships()).containsOnly(ed_knows_bob);


        // commit tran2
        transactionalGraphService.context(tran2).commit();

        // validate changes of tran2 after commit
        assertThat(transactionalGraphService.getAllPersistentNodes()).containsOnly(alice, bob, updated_Ed, frank);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).containsOnly(alice_likes_bob, bob_likes_alice, ed_knows_bob, frank_loves_gina);
//        assertThat(transactionalGraphService.getPersistentNodeById(alice.getId()).get().getRelationships()).containsOnly(alice_likes_bob, bob_likes_alice);
//        assertThat(transactionalGraphService.getPersistentNodeById(bob.getId()).get().getRelationships()).containsOnly(bob_likes_alice, alice_likes_bob, ed_knows_bob);
//        assertThat(transactionalGraphService.getPersistentNodeById(ed.getId()).get().getRelationships()).containsOnly(ed_knows_bob, ed);
    }
}
