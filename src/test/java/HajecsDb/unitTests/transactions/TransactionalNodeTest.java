package HajecsDb.unitTests.transactions;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.core.impl.NodeImpl;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;
import org.hajecsdb.graphs.transactions.transactionalGraph.TNode;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionChange;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;
import static org.hajecsdb.graphs.core.ResourceType.NODE;
import static org.hajecsdb.graphs.transactions.transactionalGraph.CRUDType.*;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalNodeTest {

    // COMMIT's Tests

    @Test
    public void addPropertyToNodeInTransactionThenCommit() {
        // given
        long transactionId = 1001;
        NodeImpl node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Adam", STRING));

        TNode tNode = new TNode(node);
        tNode.createTransactionWork(transactionId);

        // begin transaction and set new property
        TransactionChange change = new TransactionChange(NODE, CREATE, new Property("lastname", STRING, "Nowak"));
        tNode.addTransactionChange(transactionId, change);

        // when
        Node readNode = tNode.readNode(transactionId);
        assertThat(readNode.getAllProperties().getProperty("lastname").get()).isEqualTo(new Property("lastname", STRING, "Nowak"));


        // when
        tNode.commitTransaction(transactionId);

        // then
        Node originNode = tNode.getOriginNode();
        assertThat(originNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(originNode.getAllProperties().getAllProperties()).hasSize(4);
        assertThat(originNode.getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(originNode.getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, "Person"));
        assertThat(originNode.getAllProperties().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Adam"));
        assertThat(originNode.getAllProperties().getProperty("lastname").get()).isEqualTo(new Property("lastname", STRING, "Nowak"));

        assertThat(tNode.containsTransactionChanges(transactionId)).isFalse();
    }

    @Test
    public void addPropertyToNodeAndReadInTransactionThenRollback() {
        // given
        long transactionId = 1001;
        NodeImpl node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Adam", STRING));

        TNode tNode = new TNode(node);
        tNode.createTransactionWork(transactionId);

        // begin transaction and set new property
        TransactionChange change = new TransactionChange(NODE, CREATE, new Property("age", LONG, 25l));
        tNode.addTransactionChange(transactionId, change);

        // when
        Node readNode = tNode.readNode(transactionId);
        assertThat(readNode.getAllProperties().getProperty("age").get()).isEqualTo(new Property("age", LONG, 25l));


        // when
        tNode.rollbackTransaction(transactionId);

        // then
        Node originNode = tNode.getOriginNode();
        assertThat(originNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(originNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(originNode.getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(originNode.getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, "Person"));
        assertThat(originNode.getAllProperties().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Adam"));

        assertThat(tNode.containsTransactionChanges(transactionId)).isFalse();
    }

    @Test
    public void updatePropertyInNodeInTransactionThenCommit() {
        // given
        long transactionId = 1001;
        NodeImpl node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Adam", STRING));

        TNode tNode = new TNode(node);
        tNode.createTransactionWork(transactionId);

        // begin transaction and update property
        TransactionChange change = new TransactionChange(NODE, UPDATE, new Property("name", STRING, "Piotr"));
        tNode.addTransactionChange(transactionId, change);

        // when
        Node readNode = tNode.readNode(transactionId);
        assertThat(readNode.getAllProperties().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Piotr"));


        // when
        tNode.commitTransaction(transactionId);

        // then
        Node originNode = tNode.getOriginNode();
        assertThat(originNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(originNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(originNode.getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(originNode.getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, "Person"));
        assertThat(originNode.getAllProperties().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Piotr"));

        assertThat(tNode.containsTransactionChanges(transactionId)).isFalse();
    }

    @Test
    public void deletePropertyInNodeInTransactionThenCommit() {
        // given
        long transactionId = 1001;
        NodeImpl node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Adam", STRING));

        TNode tNode = new TNode(node);
        tNode.createTransactionWork(transactionId);

        // begin transaction and delete property
        TransactionChange change = new TransactionChange(NODE, DELETE, "name");
        tNode.addTransactionChange(transactionId, change);

        // when
        Node readNode = tNode.readNode(transactionId);
        assertThat(readNode.getAllProperties().hasProperty("name")).isFalse();


        // when
        tNode.commitTransaction(transactionId);

        // then
        Node originNode = tNode.getOriginNode();
        assertThat(originNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(originNode.getAllProperties().getAllProperties()).hasSize(2);
        assertThat(originNode.getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(originNode.getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, "Person"));

        assertThat(tNode.containsTransactionChanges(transactionId)).isFalse();
    }

    // ROLLBACK's Tests

    @Test
    public void addPropertyToNodeInTransactionThenRollback() {
        // given
        long transactionId = 1001;
        NodeImpl node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Adam", STRING));

        TNode tNode = new TNode(node);
        tNode.createTransactionWork(transactionId);

        // begin transaction and set new property
        TransactionChange change = new TransactionChange(NODE, CREATE, new Property("lastname", STRING, "Nowak"));
        tNode.addTransactionChange(transactionId, change);

        // when
        Node readNode = tNode.readNode(transactionId);
        assertThat(readNode.getAllProperties().getProperty("lastname").get()).isEqualTo(new Property("lastname", STRING, "Nowak"));

        // when
        tNode.rollbackTransaction(transactionId);

        // then
        Node originNode = tNode.getOriginNode();
        assertThat(originNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(originNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(originNode.getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(originNode.getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, "Person"));
        assertThat(originNode.getAllProperties().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Adam"));

        assertThat(tNode.containsTransactionChanges(transactionId)).isFalse();
    }

    @Test
    public void updatePropertyInNodeInTransactionThenRollback() {
        // given
        long transactionId = 1001;
        NodeImpl node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Adam", STRING));

        TNode tNode = new TNode(node);
        tNode.createTransactionWork(transactionId);

        // begin transaction and update property
        TransactionChange change = new TransactionChange(NODE, UPDATE, new Property("name", STRING, "Piotr"));
        tNode.addTransactionChange(transactionId, change);

        // when
        Node readNode = tNode.readNode(transactionId);
        assertThat(readNode.getAllProperties().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Piotr"));

        // when
        tNode.rollbackTransaction(transactionId);

        // then
        Node originNode = tNode.getOriginNode();
        assertThat(originNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(originNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(originNode.getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(originNode.getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, "Person"));
        assertThat(originNode.getAllProperties().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Adam"));

        assertThat(tNode.containsTransactionChanges(transactionId)).isFalse();
    }

    @Test
    public void deletePropertyInNodeInTransactionThenRollback() {
        // given
        long transactionId = 1001;
        NodeImpl node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Adam", STRING));

        TNode tNode = new TNode(node);
        tNode.createTransactionWork(transactionId);

        // begin transaction and delete property
        TransactionChange change = new TransactionChange(NODE, DELETE, "name");
        tNode.addTransactionChange(transactionId, change);

        // when
        Node readNode = tNode.readNode(transactionId);
        assertThat(readNode.getAllProperties().hasProperty("name")).isFalse();


        // when
        tNode.rollbackTransaction(transactionId);

        // then
        Node originNode = tNode.getOriginNode();
        assertThat(originNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(originNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(originNode.getAllProperties().getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(originNode.getAllProperties().getProperty("label").get()).isEqualTo(new Property("label", STRING, "Person"));
        assertThat(originNode.getAllProperties().getProperty("name").get()).isEqualTo(new Property("name", STRING, "Adam"));

        assertThat(tNode.containsTransactionChanges(transactionId)).isFalse();
    }

    // EXCEPTIONS

    @Test(expected = TransactionException.class)
    public void tryCommitTransactionWhichDoesNotExist() {
        // given
        long trueTransactionId = 1001;
        long fakeTransactionId = -1;
        NodeImpl node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Adam", STRING));

        TNode tNode = new TNode(node);
        tNode.createTransactionWork(trueTransactionId);

        // when
        tNode.commitTransaction(fakeTransactionId);
    }
}
