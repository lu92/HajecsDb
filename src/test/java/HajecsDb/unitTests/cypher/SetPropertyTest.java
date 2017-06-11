package HajecsDb.unitTests.cypher;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class SetPropertyTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void updateNamePropertyOfEmptyGraph() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'";
        Transaction transaction = transactionManager.createTransaction();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 0");

        // when
        Result result = cypherExecutor.execute(transaction, command);

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    //
    @Test
    public void updateNamePropertyOfSingleNode() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'";
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("name", "Selene", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(1);

        Node fetchedNode = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get();
        assertThat(fetchedNode.getAllProperties().size()).isEqualTo(3);
        assertThat(fetchedNode.getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(fetchedNode.getProperty("label").get()).isEqualTo(new Property("label", STRING, ""));
        assertThat(fetchedNode.getProperty("name").get()).isEqualTo(new Property("name", STRING, "Kate"));
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void updateNamePropertyOfTwoFromThreeNodes() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'";
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("name", "Selene", STRING));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("name", "Selene", STRING));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("name", "Amelia", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 2");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(3);

        Node fetchedNode1 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get();
        assertThat(fetchedNode1.getAllProperties().size()).isEqualTo(3);
        assertThat(fetchedNode1.getProperty("id").get()).isEqualTo(new Property("id", LONG, 1l));
        assertThat(fetchedNode1.getProperty("label").get()).isEqualTo(new Property("label", STRING, ""));
        assertThat(fetchedNode1.getProperty("name").get()).isEqualTo(new Property("name", STRING, "Kate"));

        Node fetchedNode2 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2).get();
        assertThat(fetchedNode2.getAllProperties().size()).isEqualTo(3);
        assertThat(fetchedNode2.getProperty("id").get()).isEqualTo(new Property("id", LONG, 2l));
        assertThat(fetchedNode2.getProperty("label").get()).isEqualTo(new Property("label", STRING, ""));
        assertThat(fetchedNode2.getProperty("name").get()).isEqualTo(new Property("name", STRING, "Kate"));

        Node fetchedNode3 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(3).get();
        assertThat(fetchedNode3.getAllProperties().size()).isEqualTo(3);
        assertThat(fetchedNode3.getProperty("id").get()).isEqualTo(new Property("id", LONG, 3l));
        assertThat(fetchedNode3.getProperty("label").get()).isEqualTo(new Property("label", STRING, ""));
        assertThat(fetchedNode3.getProperty("name").get()).isEqualTo(new Property("name", STRING, "Amelia"));

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void setLastnamePropertyOnEmptyGraph() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Andres' SET n.lastname = 'Taylor'";
        Transaction transaction = transactionManager.createTransaction();

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 0");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();
        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).isEmpty();
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andres' SET n.lastname = 'Taylor'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void setLastnamePropertyOnSingleNode() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Andres' SET n.lastname = 'Taylor'";
        Transaction transaction = transactionManager.createTransaction();
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("name", "Andres", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 1");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(1);


        Node fetchedNode = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get();
        assertThat(fetchedNode.getAllProperties().size()).isEqualTo(4);
        assertThat((Long) fetchedNode.getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat((String) fetchedNode.getProperty("label").get().getValue()).isEqualTo("");
        assertThat((String) fetchedNode.getProperty("name").get().getValue()).isEqualTo("Andres");
        assertThat((String) fetchedNode.getProperty("lastname").get().getValue()).isEqualTo("Taylor");
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) WHERE n.name = 'Andres' SET n.lastname = 'Taylor'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }

    @Test
    public void setLastnamePropertyOnThreeNode() {
        // given
        String command = "MATCH (n) SET n.lastname = 'Taylor'";
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("name", "Andres", STRING));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("age", 25l, LONG));
        cypherExecutor.getTransactionalGraphService().context(transaction).createNode(new Label(""), new Properties().add("university", "UJ", STRING));

        ResultRow expectedResultRow = new ResultRow();
        expectedResultRow.setContentType(ContentType.STRING);
        expectedResultRow.setMessage("Properties set: 3");

        // when
        Result result = cypherExecutor.execute(transaction, command);
        cypherExecutor.getTransactionalGraphService().context(transaction).commit();

        //then
        assertThat(cypherExecutor.getTransactionalGraphService().getAllPersistentNodes()).hasSize(3);

        Node fetchedNode1 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(1).get();
        assertThat(fetchedNode1.getAllProperties().size()).isEqualTo(4);
        assertThat((Long) fetchedNode1.getProperty("id").get().getValue()).isEqualTo(1l);
        assertThat((String) fetchedNode1.getProperty("label").get().getValue()).isEqualTo("");
        assertThat((String) fetchedNode1.getProperty("name").get().getValue()).isEqualTo("Andres");
        assertThat((String) fetchedNode1.getProperty("lastname").get().getValue()).isEqualTo("Taylor");

        Node fetchedNode2 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(2).get();
        assertThat(fetchedNode2.getAllProperties().size()).isEqualTo(4);
        assertThat((Long) fetchedNode2.getProperty("id").get().getValue()).isEqualTo(2l);
        assertThat((String) fetchedNode2.getProperty("label").get().getValue()).isEqualTo("");
        assertThat((Long) fetchedNode2.getProperty("age").get().getValue()).isEqualTo(25l);
        assertThat((String) fetchedNode2.getProperty("lastname").get().getValue()).isEqualTo("Taylor");

        Node fetchedNode3 = cypherExecutor.getTransactionalGraphService().getPersistentNodeById(3).get();
        assertThat(fetchedNode3.getAllProperties().size()).isEqualTo(4);
        assertThat((Long) fetchedNode3.getProperty("id").get().getValue()).isEqualTo(3l);
        assertThat((String) fetchedNode3.getProperty("label").get().getValue()).isEqualTo("");
        assertThat((String) fetchedNode3.getProperty("university").get().getValue()).isEqualTo("UJ");
        assertThat((String) fetchedNode3.getProperty("lastname").get().getValue()).isEqualTo("Taylor");

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo("MATCH (n) SET n.lastname = 'Taylor'");
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(expectedResultRow.getContentType());
        assertThat(result.getResults().get(0).getMessage()).isEqualTo(expectedResultRow.getMessage());
    }
}
