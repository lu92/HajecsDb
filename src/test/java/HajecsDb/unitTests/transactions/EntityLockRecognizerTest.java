package HajecsDb.unitTests.transactions;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.transactions.lockMechanism.EntityLockRecognizer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class EntityLockRecognizerTest {

    private EntityLockRecognizer entityLockRecognizer = new EntityLockRecognizer();
    private CypherExecutor cypherExecutor = new CypherExecutor();
    private Graph graph;

    public EntityLockRecognizerTest() {
        graph = new GraphImpl("pathDir", "graphDir");
        cypherExecutor.execute(graph, "CREATE (p: Vampire {name: 'Selene'})");
        cypherExecutor.execute(graph, "CREATE (p: Vampire {name: 'Victor'})");
        cypherExecutor.execute(graph, "CREATE (p: Hibrid {name: 'Marcus'})");
        cypherExecutor.execute(graph, "CREATE (p: Hibrid {name: 'Michael'})");
        cypherExecutor.execute(graph, "CREATE (p: Vampire {name: 'Kraven'})");
        cypherExecutor.execute(graph, "CREATE (p: Vampire {name: 'Tanis'})");
        cypherExecutor.execute(graph, "CREATE (p: Lykan {name: 'William'})");
        cypherExecutor.execute(graph, "MATCH (m {name: 'Marcus'}) MATCH (s {name: 'Selene'}) CREATE (m)-[p:KNOW]->(s)");
        cypherExecutor.execute(graph, "MATCH (m {name: 'Selene'}) MATCH (s {name: 'Tanis'}) CREATE (m)-[p:KNOW]->(s)");
        cypherExecutor.execute(graph, "MATCH (v {name: 'Victor'}) MATCH (s {name: 'Selene'}) CREATE (v)-[p:LIKES]->(s)");
        cypherExecutor.execute(graph, "MATCH (v {name: 'Victor'}) MATCH (s {name: 'Kraven'}) CREATE (v)-[p:LIKES]->(s)");
    }

    @Test
    public void qualifyNodesByLabelInEmptyGraphTest() {
        // given
        String query = "MATCH (n: Vampire) SET n.likeBlood = 'yes'";
        Graph emptyGraph = new GraphImpl("test", "test");

        // when
        List<Entity> entities = entityLockRecognizer.determineEntities(emptyGraph, query);

        // then
        assertThat(entities).isEmpty();
    }

    @Test
    public void qualifyNodesByLabelTest() {
        // given
        String query = "MATCH (n: Vampire) SET n.likeBlood = 'yes'";

        // when
        List<Entity> entities = entityLockRecognizer.determineEntities(graph, query);

        // then
        assertThat(entities).hasSize(4);
        assertThat(entities).containsExactly(graph.getNodeById(1).get(), graph.getNodeById(2).get(),
                graph.getNodeById(5).get(), graph.getNodeById(6).get());
    }
}
