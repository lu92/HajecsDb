package HajecsDb.unitTests.cypher;

import org.hajecsdb.graphs.cypher.clauses.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClausesSeparator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Stack;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum.*;

@RunWith(MockitoJUnitRunner.class)
public class ClausesSeparatorTest {

    private ClausesSeparator clausesSeparator = new ClausesSeparator();

    @Test
    public void splitCreateNodeClauseTest() {
        // given
        String command = "CREATE (n: Person) RETURN n";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(2);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(CREATE_NODE, "(n:Person)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitMatchNodeClauseTest() {
        // given
        String command = "MATCH (n: Person) RETURN n";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(2);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n:Person)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitWhereClauseTest() {
        // given
        String command = "MATCH (n: Person) WHERE n.age > 25 RETURN n";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(3);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n:Person)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(WHERE, "n.age>25"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitMatchWithRemoveClauseTest() {
        // given
        String command = "MATCH (n) REMOVE n.age";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(2);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(REMOVE, "n.age"));
        //        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitMatchWithRemoveClauseTest2() {
        // given
        String command = "MATCH (n: Person) REMOVE n:Person";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(2);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n:Person)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(REMOVE, "n:Person"));
//        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitMatchAndWhereWithRemoveClauseTest() {
        // given
        String command = "MATCH (n) WHERE n.age > 25 REMOVE n.age";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(3);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(WHERE, "n.age>25"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(REMOVE, "n.age"));
        //        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitDeleteClauseTest() {
        // given
        String command = "MATCH (n) DELETE n";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(2);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(DELETE, "n"));
//        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitWhereWhereAndDeleteClauseTest() {
        // given
        String command = "MATCH (n) WHERE n.age > 25 DELETE n";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(3);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(WHERE, "n.age>25"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(DELETE, "n"));
//        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitMatchWhereAndSetClauseTest() {
        // given
        String command = "MATCH (n) WHERE n.name = 'Selene' SET n.name = 'Kate'";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(3);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(WHERE, "n.name='Selene'"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(SET, "n.name='Kate'"));
//        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitDoubleMatchAndCreateClauseTest() {
        // given
        StringBuilder commandBuilder = new StringBuilder()
                .append("MATCH (u:User) ")
                .append("MATCH (r:Role) ")
                .append("CREATE (u)-[rel:HAS_ROLE]->(r)");

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(commandBuilder.toString());

        // then
        assertThat(clauseInvocationStack).hasSize(3);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(u:User)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(r:Role)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(CREATE_RELATIONSHIP, "(u)-[rel:HAS_ROLE]->(r)"));
//        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitMatchWithParameterClauseTest() {
        // given
        String command = "MATCH (n: Person {name : 'Adam'})";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(1);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_NODE, "(n:Person{name:'Adam'})"));
//        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

    @Test
    public void splitMatchRelationshipClauseTest() {
        // given
        String command = "MATCH (director: Person { name: 'Johnson' })--(movie)";

        // when
        Stack<ClauseInvocation> clauseInvocationStack = clausesSeparator.splitByClauses(command);

        // then
        assertThat(clauseInvocationStack).hasSize(1);
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH_RELATIONSHIP, "(director:Person{name:'Johnson'})--(movie)"));
//        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }

}
