package HajecsDb.unitTests.cypher;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.*;

@RunWith(MockitoJUnitRunner.class)
public class CreateNodeTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();

    @Test
    public void createEmptyNodeWithLabelTest() {
        // given
        String command = "CREATE (n: Person) RETURN n";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(ContentType.NODE);
        assertThat(result.getResults().get(0).getNode()).isEqualTo(transactionalGraphService.getPersistentNodeById(1).get());

        assertThat(transactionalGraphService.getAllPersistentNodes().size()).isEqualTo(1);
        Node fetchedNode = transactionalGraphService.getPersistentNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l), new Property("label", STRING, "Person"));
    }

    @Test
    public void createNodeWithIntParameterTest() {
        // given
        String command = "CREATE (n: Person {age: 25}) RETURN n";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(ContentType.NODE);
        assertThat(result.getResults().get(0).getNode()).isEqualTo(transactionalGraphService.getPersistentNodeById(1).get());

        assertThat(transactionalGraphService.getAllPersistentNodes().size()).isEqualTo(1);
        Node fetchedNode = transactionalGraphService.getPersistentNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("age", INT, 25));

    }

    @Test
    public void createNodeWithStringParameterTest() {
        // given
        String command = "CREATE (n: Person {name: 'Peter'}) RETURN n";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(ContentType.NODE);
        assertThat(result.getResults().get(0).getNode()).isEqualTo(transactionalGraphService.getPersistentNodeById(1).get());

        assertThat(transactionalGraphService.getAllPersistentNodes().size()).isEqualTo(1);
        Node fetchedNode = transactionalGraphService.getPersistentNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties()).hasSize(3);
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("name", STRING, "Peter"));
    }

    @Test
    public void createNodeWithTwoParametersTest() {
        // given
        String command = "CREATE (n: Person {firstName: 'Jan', lastName: 'Kowalski'}) RETURN n";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(ContentType.NODE);
        assertThat(result.getResults().get(0).getNode()).isEqualTo(transactionalGraphService.getPersistentNodeById(1).get());

        assertThat(transactionalGraphService.getAllPersistentNodes().size()).isEqualTo(1);
        Node fetchedNode = transactionalGraphService.getPersistentNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties()).hasSize(4);
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("firstName", STRING, "Jan"),
                        new Property("lastName", STRING, "Kowalski"));
    }

    @Test
    public void createNodeWithTreeParametersTest() {
        // given
        String command = "CREATE (n: Person {firstName: 'Jan', salary: 3000.00, age: 40}) RETURN n";
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, command);
        transactionalGraphService.context(transaction).commit();

        // then
        assertThat(result.isCompleted()).isTrue();
        assertThat(result.getCommand()).isEqualTo(command);
        assertThat(result.getResults()).hasSize(1);
        assertThat(result.getResults().get(0).getContentType()).isEqualTo(ContentType.NODE);
        assertThat(result.getResults().get(0).getNode()).isEqualTo(transactionalGraphService.getPersistentNodeById(1).get());

        assertThat(transactionalGraphService.getAllPersistentNodes().size()).isEqualTo(1);
        Node fetchedNode = transactionalGraphService.getPersistentNodeById(1l).get();
        assertThat(fetchedNode.getLabel()).isEqualTo(new Label("Person"));
        assertThat(fetchedNode.getAllProperties().getAllProperties()).hasSize(5);
        assertThat(fetchedNode.getAllProperties().getAllProperties())
                .containsOnly(new Property("id", LONG, 1l),
                        new Property("label", STRING, "Person"),
                        new Property("firstName", STRING, "Jan"),
                        new Property("salary", DOUBLE, 3000.00),
                        new Property("age", INT, 40));
    }
}
