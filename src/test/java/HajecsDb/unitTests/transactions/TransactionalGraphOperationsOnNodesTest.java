package HajecsDb.unitTests.transactions;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.restLayer.Session;
import org.hajecsdb.graphs.restLayer.SessionPool;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalGraphOperationsOnNodesTest {

    private TransactionManager transactionManager = new TransactionManager();
    private SessionPool sessionPool = new SessionPool();

    @Test
    public void createNodeInTransactionAndCommitTest() {

        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction tran1 = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // when
        transactionalGraphService.context(tran1).createNode(new Label("Person"), null);
        transactionalGraphService.context(tran1).commit();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        Optional<Node> nodeOptional = transactionalGraphService.getPersistentNodeById(1);
        assertThat(nodeOptional.get().getId()).isEqualTo(1);
        assertThat(nodeOptional.get().getLabel()).isEqualTo(new Label("Person"));
        assertThat(nodeOptional.get().getRelationships()).isEmpty();
    }

    @Test
    public void createNodeInTransactionAndRollbackTest() {

        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction tran1 = session.beginTransaction();
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // when
        transactionalGraphService.context(tran1).createNode(new Label("Person"), null);
        transactionalGraphService.context(tran1).rollback();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
    }

    @Test
    public void deleteNodeInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).createNode(new Label("Person"), null);
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isTrue();


        // when
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).deleteNode(1);
        transactionalGraphService.context(tran2).commit();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
    }

    @Test
    public void deleteNodeInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).createNode(new Label("Person"), null);
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isTrue();


        // when
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).deleteNode(1);
        transactionalGraphService.context(tran2).rollback();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        Optional<Node> nodeOptional = transactionalGraphService.getPersistentNodeById(1);
        assertThat(nodeOptional.get().getId()).isEqualTo(1);
        assertThat(nodeOptional.get().getLabel()).isEqualTo(new Label("Person"));
        assertThat(nodeOptional.get().getRelationships()).isEmpty();
    }


    @Test
    public void addPropertyToNodeInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).createNode(new Label("Person"), null);
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isTrue();

        //when
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).setPropertyToNode(1, new Property("name", STRING, "Adam"));
        transactionalGraphService.context(tran2).commit();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        Optional<Node> nodeOptional = transactionalGraphService.getPersistentNodeById(1);
        assertThat(nodeOptional.get().getId()).isEqualTo(1);
        assertThat(nodeOptional.get().getLabel()).isEqualTo(new Label("Person"));
        assertThat(nodeOptional.get().hasProperty("name")).isTrue();
        assertThat(nodeOptional.get().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Adam"));
        assertThat(nodeOptional.get().getRelationships()).isEmpty();
    }

    @Test
    public void addPropertyToNodeInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).createNode(new Label("Person"), null);
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isTrue();

        //when
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).setPropertyToNode(1, new Property("name", STRING, "Adam"));
        transactionalGraphService.context(tran2).rollback();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        Optional<Node> nodeOptional = transactionalGraphService.getPersistentNodeById(1);
        assertThat(nodeOptional.get().getId()).isEqualTo(1);
        assertThat(nodeOptional.get().getLabel()).isEqualTo(new Label("Person"));
        assertThat(nodeOptional.get().hasProperty("name")).isFalse();
        assertThat(nodeOptional.get().getRelationships()).isEmpty();
    }

    @Test
    public void updatePropertyToNodeInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).createNode(new Label("Person"), new Properties().add("name", "Adam", STRING));
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isTrue();

        //when
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).setPropertyToNode(1, new Property("name", STRING, "Bob"));
        Node readedNode = transactionalGraphService.context(tran2).getNodeById(1).get();
        assertThat(readedNode.getProperty("name").get()).isEqualTo(new Property("name", STRING, "Bob"));
        transactionalGraphService.context(tran2).commit();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        Optional<Node> nodeOptional = transactionalGraphService.getPersistentNodeById(1);
        assertThat(nodeOptional.get().getId()).isEqualTo(1);
        assertThat(nodeOptional.get().getLabel()).isEqualTo(new Label("Person"));
        assertThat(nodeOptional.get().hasProperty("name")).isTrue();
        assertThat(nodeOptional.get().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Bob"));
        assertThat(nodeOptional.get().getRelationships()).isEmpty();
    }

    @Test
    public void updatePropertyToNodeInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).createNode(new Label("Person"), new Properties().add("name", "Adam", STRING));
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isTrue();

        //when
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).setPropertyToNode(1, new Property("name", STRING, "Bob"));
        Node readedNode = transactionalGraphService.context(tran2).getNodeById(1).get();
        assertThat(readedNode.getProperty("name").get()).isEqualTo(new Property("name", STRING, "Bob"));
        transactionalGraphService.context(tran2).rollback();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        Optional<Node> nodeOptional = transactionalGraphService.getPersistentNodeById(1);
        assertThat(nodeOptional.get().getId()).isEqualTo(1);
        assertThat(nodeOptional.get().getLabel()).isEqualTo(new Label("Person"));
        assertThat(nodeOptional.get().hasProperty("name")).isTrue();
        assertThat(nodeOptional.get().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Adam"));
        assertThat(nodeOptional.get().getRelationships()).isEmpty();
    }

    @Test
    public void deletePropertyToNodeInTransactionAndCommitTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).createNode(new Label("Person"), new Properties().add("name", "Adam", STRING));
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isTrue();

        //when
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).deletePropertyFromNode(1, "name");
        Node readedNode = transactionalGraphService.context(tran2).getNodeById(1).get();
        assertThat(readedNode.hasProperty("name")).isFalse();
        transactionalGraphService.context(tran2).commit();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        Optional<Node> nodeOptional = transactionalGraphService.getPersistentNodeById(1);
        assertThat(nodeOptional.get().getId()).isEqualTo(1);
        assertThat(nodeOptional.get().getLabel()).isEqualTo(new Label("Person"));
        assertThat(nodeOptional.get().hasProperty("name")).isFalse();
        assertThat(nodeOptional.get().getRelationships()).isEmpty();
    }

    @Test
    public void deletePropertyToNodeInTransactionAndRollbackTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).createNode(new Label("Person"), new Properties().add("name", "Adam", STRING));
        transactionalGraphService.context(tran1).commit();
        assertThat(transactionalGraphService.getPersistentNodeById(1).isPresent()).isTrue();

        //when
        Transaction tran2 = session.beginTransaction();
        transactionalGraphService.context(tran2).deletePropertyFromNode(1, "name");
        Node readedNode = transactionalGraphService.context(tran2).getNodeById(1).get();
        assertThat(readedNode.hasProperty("name")).isFalse();
        transactionalGraphService.context(tran2).rollback();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(1);
        Optional<Node> nodeOptional = transactionalGraphService.getPersistentNodeById(1);
        assertThat(nodeOptional.get().getId()).isEqualTo(1);
        assertThat(nodeOptional.get().getLabel()).isEqualTo(new Label("Person"));
        assertThat(nodeOptional.get().hasProperty("name")).isTrue();
        assertThat(nodeOptional.get().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Adam"));
        assertThat(nodeOptional.get().getRelationships()).isEmpty();
    }

    @Test(expected = NotFoundException.class)
    public void tryToDeleteNodeWhichNotExistInTransactionTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // when
        Transaction tran1 = session.beginTransaction();
        transactionalGraphService.context(tran1).deleteNode(-1);
        transactionalGraphService.context(tran1).commit();

        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).isEmpty();
    }

    @Test
    public void tryToGetPersistentNodeWhichDoesNotExistTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        // when
        Optional<Node> persistentNode = transactionalGraphService.getPersistentNodeById(-1);

        assertThat(persistentNode.isPresent()).isFalse();
    }

    @Test
    public void contextWithNullTransactionShouldThrowExceptionTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        try {
            // when
            transactionalGraphService.context(null).commit();
        } catch (TransactionException e) {
            // then
            assertThat(e.getMessage()).isEqualTo("Not defined transaction!");
        }
    }

    @Test
    public void notInitiatedTransactionFromSessionExpectedExceptionTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        try {
            // when
            session.getTransaction();
        } catch (TransactionException e) {
            // then
            assertThat(e.getMessage()).isEqualTo("The transaction has not been initiated!");
        }
    }

    @Test
    public void doubleCommitingOfTransactionCausesExceptionTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction transaction = session.beginTransaction();
        transactionalGraphService.context(transaction).commit();
        try {
            // when
            transactionalGraphService.context(transaction).commit();
        } catch (TransactionException e) {
            // then
            assertThat(e.getMessage()).isEqualTo("Transaction was performed! [COMMITED OR ROLLBACKED]");
        }
    }

    @Test
    public void rollbackCommittedTransactionCausesExceptionTest() {
        // given
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

        Transaction transaction = session.beginTransaction();
        transactionalGraphService.context(transaction).commit();
        try {
            // when
            transactionalGraphService.context(transaction).rollback();
        } catch (TransactionException e) {
            // then
            assertThat(e.getMessage()).isEqualTo("Transaction was performed! [COMMITED OR ROLLBACKED]");
        }
    }

}
