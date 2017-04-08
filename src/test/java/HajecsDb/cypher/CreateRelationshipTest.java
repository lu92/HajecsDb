package HajecsDb.cypher;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.impl.GraphImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class CreateRelationshipTest {

    private Graph graph;
    private CypherExecutor cypherExecutor;

    @Test
    public void createTwoNodesAndConnectThem() {

        // given
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        cypherExecutor.execute("CREATE (u: User {username:'admin'})");
        cypherExecutor.execute("CREATE (r: Role {name:'ROLE_WEB_USER'})");

        StringBuilder commandBuilder = new StringBuilder()
                .append("MATCH (u: User) ")
                .append("MATCH (r:Role) ")
                .append("CREATE (u)-[p:HAS_ROLE]->(r)");


        // when
        Result result = cypherExecutor.execute(commandBuilder.toString());


        // then
        assertThat(graph.getAllNodes()).hasSize(2);
        assertThat(graph.getAllRelationships()).hasSize(2);
        assertThat(graph.findRelationship(1, 2, new Label("HAS_ROLE"))).isNotNull();
        try {
            graph.findRelationship(2, 1, new Label("HAS_ROLE"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Relationship does not exist!");
        }
    }

    @Test
    public void createSixNodesAndConnectThem() {

        // given
        graph = new GraphImpl("pathDir", "graphName");
        cypherExecutor = new CypherExecutor(graph);

        cypherExecutor.execute("CREATE (u: User {username:'admin'})");
        cypherExecutor.execute("CREATE (u: User {username:'userA'})");
        cypherExecutor.execute("CREATE (u: User {username:'userB'})");
        cypherExecutor.execute("CREATE (g: Guest {username:'guest1'})");
        cypherExecutor.execute("CREATE (g: Guest {username:'guest2'})");
        cypherExecutor.execute("CREATE (r: Role {name:'ROLE_USER'})");

        StringBuilder commandBuilder = new StringBuilder()
                .append("MATCH (u: User) ")
                .append("MATCH (r:Role) ")
                .append("CREATE (u)-[p:HAS_ROLE]->(r)");

        // when
        Result result = cypherExecutor.execute(commandBuilder.toString());


        // then
        assertThat(graph.getAllNodes()).hasSize(6);
        assertThat(graph.getAllRelationships()).hasSize(6);
//        assertThat(graph.findRelationship(1, 2, new RelationshipType("HAS_ROLE"))).isNotNull();
//        try {
//            graph.findRelationship(2, 1, new RelationshipType("HAS_ROLE"));
//        } catch (IllegalArgumentException e) {
//            assertThat(e.getMessage()).isEqualTo("Relationship does not exist!");
//        }
    }
}
