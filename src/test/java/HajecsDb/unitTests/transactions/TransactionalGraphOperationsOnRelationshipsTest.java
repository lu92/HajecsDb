package HajecsDb.unitTests.transactions;

import HajecsDb.unitTests.utils.NodeComparator;
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
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalGraphOperationsOnRelationshipsTest {

    private TransactionManager transactionManager = new TransactionManager();
    private SessionPool sessionPool = new SessionPool();
    NodeComparator nodeComparator = new NodeComparator();


    @Test
    public void createRelationshipInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));

        transactionalGraphService.context(transaction).commit();

        assertThat(transactionalGraphService.isEntityLocked(alice)).isFalse();
        assertThat(transactionalGraphService.isEntityLocked(gina)).isFalse();
        assertThat(transactionalGraphService.isEntityLocked(bob)).isFalse();

        // when
        Transaction tran1 = session.beginTransaction();
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(tran1).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));

        assertThat(transactionalGraphService.context(tran1).getTNodeById(tran1.getId(), bob.getId()).get().containsTransactionChanges(tran1.getId())).isFalse();
        assertThat(transactionalGraphService.context(tran1).getTNodeById(tran1.getId(), alice.getId()).get().containsTransactionChanges(tran1.getId())).isTrue();
        assertThat(transactionalGraphService.context(tran1).getTNodeById(tran1.getId(), gina.getId()).get().containsTransactionChanges(tran1.getId())).isTrue();

        assertThat(transactionalGraphService.isEntityLocked(alice)).isTrue();
        assertThat(transactionalGraphService.isEntityLocked(gina)).isTrue();
        assertThat(transactionalGraphService.isEntityLocked(bob)).isFalse();


        assertThat(alice_knows_gina_relationship.getId()).isEqualTo(4);
        assertThat(alice_knows_gina_relationship.getStartNode()).isEqualTo(alice);
        assertThat(alice_knows_gina_relationship.getEndNode()).isEqualTo(gina);
        assertThat(alice_knows_gina_relationship.getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(alice_knows_gina_relationship.getDirection()).isEqualTo(Direction.OUTGOING);

        transactionalGraphService.context(tran1).commit();

        // then
        Optional<Relationship> relationshipOptional = transactionalGraphService.getPersistentRelationshipById(4);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getId()).isEqualTo(4);
        assertThat(relationshipOptional.get().getStartNode()).isEqualTo(alice);
        assertThat(relationshipOptional.get().getEndNode()).isEqualTo(gina);
        assertThat(relationshipOptional.get().getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(relationshipOptional.get().getDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(transactionalGraphService.getPersistentNodeById(1).get().getDegree()).isEqualTo(1);
        assertThat(transactionalGraphService.getPersistentNodeById(2).get().getDegree()).isEqualTo(1);
        assertThat(transactionalGraphService.getPersistentNodeById(3).get().getDegree()).isEqualTo(0);
    }

    @Test
    public void createRelationshipInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));
        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));

        transactionalGraphService.context(transaction).commit();

        assertThat(transactionalGraphService.isEntityLocked(alice)).isFalse();
        assertThat(transactionalGraphService.isEntityLocked(gina)).isFalse();
        assertThat(transactionalGraphService.isEntityLocked(bob)).isFalse();


        // when
        Transaction tran1 = session.beginTransaction();
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(tran1).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));

        assertThat(alice_knows_gina_relationship.getId()).isEqualTo(4);
        assertThat(alice_knows_gina_relationship.getStartNode()).isEqualTo(alice);
        assertThat(alice_knows_gina_relationship.getEndNode()).isEqualTo(gina);
        assertThat(alice_knows_gina_relationship.getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(alice_knows_gina_relationship.getDirection()).isEqualTo(Direction.OUTGOING);



        transactionalGraphService.context(tran1).rollback();

        // then
        assertThat(transactionalGraphService.getPersistentRelationshipById(4).isPresent()).isFalse();
        assertThat(transactionalGraphService.getPersistentNodeById(1).get().getDegree()).isEqualTo(0);
        assertThat(transactionalGraphService.getPersistentNodeById(2).get().getDegree()).isEqualTo(0);
        assertThat(transactionalGraphService.getPersistentNodeById(3).get().getDegree()).isEqualTo(0);

        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(3);
        assertThat(nodeComparator.isSame(alice, transactionalGraphService.getPersistentNodeById(1).get())).isTrue();
        assertThat(nodeComparator.isSame(gina, transactionalGraphService.getPersistentNodeById(2).get())).isTrue();
        assertThat(nodeComparator.isSame(bob, transactionalGraphService.getPersistentNodeById(3).get())).isTrue();
    }

    @Test
    public void addPropertyToRelationshipInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        transactionalGraphService.context(transaction).commit();



        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).setPropertyToRelationship(4, new Property("since", STRING, "1997"));
        transactionalGraphService.context(tran1).commit();

        // then
        Optional<Relationship> relationshipOptional = transactionalGraphService.getPersistentRelationshipById(4);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getId()).isEqualTo(4);
        assertThat(relationshipOptional.get().getStartNode()).isEqualTo(alice);
        assertThat(relationshipOptional.get().getEndNode()).isEqualTo(gina);
        assertThat(relationshipOptional.get().getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(relationshipOptional.get().getDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(relationshipOptional.get().hasProperty("since")).isTrue();
        assertThat(relationshipOptional.get().getProperty("since").get()).isEqualTo(new Property("since", STRING, "1997"));
    }

    @Test
    public void addPropertyToRelationshipInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        transactionalGraphService.context(transaction).commit();


        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).setPropertyToRelationship(4, new Property("since", STRING, "1997"));
        transactionalGraphService.context(tran1).rollback();

        // then
        Optional<Relationship> relationshipOptional = transactionalGraphService.getPersistentRelationshipById(4);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getId()).isEqualTo(4);
        assertThat(relationshipOptional.get().getStartNode()).isEqualTo(alice);
        assertThat(relationshipOptional.get().getEndNode()).isEqualTo(gina);
        assertThat(relationshipOptional.get().getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(relationshipOptional.get().getDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(relationshipOptional.get().hasProperty("since")).isFalse();
    }

    @Test
    public void updatePropertyToRelationshipInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        transactionalGraphService.context(transaction).setPropertyToRelationship(4, new Property("since", STRING, "1997"));
        transactionalGraphService.context(transaction).commit();



        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).setPropertyToRelationship(4, new Property("since", STRING, "2007"));
        transactionalGraphService.context(tran1).commit();

        // then
        Optional<Relationship> relationshipOptional = transactionalGraphService.getPersistentRelationshipById(4);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getId()).isEqualTo(4);
        assertThat(relationshipOptional.get().getStartNode()).isEqualTo(alice);
        assertThat(relationshipOptional.get().getEndNode()).isEqualTo(gina);
        assertThat(relationshipOptional.get().getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(relationshipOptional.get().getDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(relationshipOptional.get().hasProperty("since")).isTrue();
        assertThat(relationshipOptional.get().getProperty("since").get()).isEqualTo(new Property("since", STRING, "2007"));
    }

    @Test
    public void updatePropertyToRelationshipInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        transactionalGraphService.context(transaction).setPropertyToRelationship(4, new Property("since", STRING, "1997"));
        transactionalGraphService.context(transaction).commit();



        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).setPropertyToRelationship(4, new Property("since", STRING, "2007"));
        transactionalGraphService.context(tran1).rollback();

        // then
        Optional<Relationship> relationshipOptional = transactionalGraphService.getPersistentRelationshipById(4);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getId()).isEqualTo(4);
        assertThat(relationshipOptional.get().getStartNode()).isEqualTo(alice);
        assertThat(relationshipOptional.get().getEndNode()).isEqualTo(gina);
        assertThat(relationshipOptional.get().getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(relationshipOptional.get().getDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(relationshipOptional.get().hasProperty("since")).isTrue();
        assertThat(relationshipOptional.get().getProperty("since").get()).isEqualTo(new Property("since", STRING, "1997"));
    }

    @Test
    public void deletePropertyToRelationshipInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        transactionalGraphService.context(transaction).setPropertyToRelationship(4, new Property("since", STRING, "1997"));
        transactionalGraphService.context(transaction).commit();



        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).deletePropertyFromRelationship(4, "since");
        transactionalGraphService.context(tran1).commit();

        // then
        Optional<Relationship> relationshipOptional = transactionalGraphService.getPersistentRelationshipById(4);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getId()).isEqualTo(4);
        assertThat(relationshipOptional.get().getStartNode()).isEqualTo(alice);
        assertThat(relationshipOptional.get().getEndNode()).isEqualTo(gina);
        assertThat(relationshipOptional.get().getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(relationshipOptional.get().getDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(relationshipOptional.get().hasProperty("since")).isFalse();
    }

    @Test
    public void deletePropertyToRelationshipInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        transactionalGraphService.context(transaction).setPropertyToRelationship(4, new Property("since", STRING, "1997"));
        transactionalGraphService.context(transaction).commit();



        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).deletePropertyFromRelationship(4, "since");
        transactionalGraphService.context(tran1).rollback();

        // then
        Optional<Relationship> relationshipOptional = transactionalGraphService.getPersistentRelationshipById(4);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getId()).isEqualTo(4);
        assertThat(relationshipOptional.get().getStartNode()).isEqualTo(alice);
        assertThat(relationshipOptional.get().getEndNode()).isEqualTo(gina);
        assertThat(relationshipOptional.get().getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(relationshipOptional.get().getDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(relationshipOptional.get().hasProperty("since")).isTrue();
        assertThat(relationshipOptional.get().getProperty("since").get()).isEqualTo(new Property("since", STRING, "1997"));    }

    @Test
    public void deleteRelationshipInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        transactionalGraphService.context(transaction).setPropertyToRelationship(4, new Property("since", STRING, "1997"));
        transactionalGraphService.context(transaction).commit();

        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).deleteRelationship(alice_knows_gina_relationship.getId());
        transactionalGraphService.context(tran1).commit();

        // then
        assertThat(transactionalGraphService.getPersistentRelationshipById(alice_knows_gina_relationship.getId()).isPresent()).isFalse();
        assertThat(transactionalGraphService.getPersistentNodeById(1).get().getDegree()).isEqualTo(0);
        assertThat(transactionalGraphService.getPersistentNodeById(2).get().getDegree()).isEqualTo(0);
        assertThat(transactionalGraphService.getPersistentNodeById(3).get().getDegree()).isEqualTo(0);
    }

    @Test
    public void deleteRelationshipInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // transaction - provide default state of graph
        Node alice = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Alice")));
        Node gina = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Gina")));

        Node bob = transactionalGraphService.context(transaction)
                .createNode(new Label("Person"), new Properties().add(new Property("name", STRING, "Bob")));
        Relationship alice_knows_gina_relationship =
                transactionalGraphService.context(transaction).createRelationship(alice.getId(), gina.getId(), new Label("KNOWS"));
        transactionalGraphService.context(transaction).setPropertyToRelationship(4, new Property("since", STRING, "1997"));
        transactionalGraphService.context(transaction).commit();

        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).deleteRelationship(alice_knows_gina_relationship.getId());
        transactionalGraphService.context(tran1).rollback();

        // then
        Optional<Relationship> relationshipOptional = transactionalGraphService.getPersistentRelationshipById(4);
        assertThat(relationshipOptional.isPresent()).isTrue();
        assertThat(relationshipOptional.get().getId()).isEqualTo(4);
        assertThat(relationshipOptional.get().getStartNode()).isEqualTo(alice);
        assertThat(relationshipOptional.get().getEndNode()).isEqualTo(gina);
        assertThat(relationshipOptional.get().getLabel()).isEqualTo(new Label("KNOWS"));
        assertThat(relationshipOptional.get().getDirection()).isEqualTo(Direction.OUTGOING);
        assertThat(relationshipOptional.get().hasProperty("since")).isTrue();
        assertThat(relationshipOptional.get().getProperty("since").get()).isEqualTo(new Property("since", STRING, "1997"));
    }

}
