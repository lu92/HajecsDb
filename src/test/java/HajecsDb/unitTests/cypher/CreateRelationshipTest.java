package HajecsDb.unitTests.cypher;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class CreateRelationshipTest {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private TransactionManager transactionManager = new TransactionManager();


    @Test
    public void createTwoNodesAndConnectThem() {

        // given
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (u: User {username:'admin'})");
        cypherExecutor.execute(transactionalGraphService, transaction, "CREATE (r: Role {name:'ROLE_WEB_USER'})");


        StringBuilder commandBuilder = new StringBuilder()
                .append("MATCH (u: User) ")
                .append("MATCH (r:Role) ")
                .append("CREATE (u)-[p:HAS_ROLE]->(r)");


        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, commandBuilder.toString());
        transactionalGraphService.context(transaction).commit();


        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(2);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).hasSize(1);
//        assertThat(transactionalGraphService.findRelationship(1, 2, new Label("HAS_ROLE"))).isNotNull();
//        try {
//            graph.findRelationship(2, 1, new Label("HAS_ROLE"));
//        } catch (IllegalArgumentException e) {
//            assertThat(e.getMessage()).isEqualTo("Relationship does not exist!");
//        }
    }

    @Test
    public void createSixNodesAndConnectThem() {

        // given
        TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
        Transaction transaction = transactionManager.createTransaction();

        cypherExecutor.execute(transactionalGraphService, transaction,"CREATE (u: User {username:'admin'})");
        cypherExecutor.execute(transactionalGraphService, transaction,"CREATE (u: User {username:'userA'})");
        cypherExecutor.execute(transactionalGraphService, transaction,"CREATE (u: User {username:'userB'})");
        cypherExecutor.execute(transactionalGraphService, transaction,"CREATE (g: Guest {username:'guest1'})");
        cypherExecutor.execute(transactionalGraphService, transaction,"CREATE (g: Guest {username:'guest2'})");
        cypherExecutor.execute(transactionalGraphService, transaction,"CREATE (r: Role {name:'ROLE_USER'})");

        StringBuilder commandBuilder = new StringBuilder()
                .append("MATCH (u: User) ")
                .append("MATCH (r:Role) ")
                .append("CREATE (u)-[p:HAS_ROLE]->(r)");

        // when
        Result result = cypherExecutor.execute(transactionalGraphService, transaction, commandBuilder.toString());
        transactionalGraphService.context(transaction).commit();


        // then
        assertThat(transactionalGraphService.getAllPersistentNodes()).hasSize(6);
        assertThat(transactionalGraphService.getAllPersistentRelationships()).hasSize(3);
//        assertThat(graph.findRelationship(1, 2, new RelationshipType("HAS_ROLE"))).isNotNull();
//        try {
//            graph.findRelationship(2, 1, new RelationshipType("HAS_ROLE"));
//        } catch (IllegalArgumentException e) {
//            assertThat(e.getMessage()).isEqualTo("Relationship does not exist!");
//        }
    }
}
