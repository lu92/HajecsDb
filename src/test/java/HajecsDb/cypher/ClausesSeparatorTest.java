package HajecsDb.cypher;

import org.hajecsdb.graphs.cypher.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.DFA.ClausesSeparator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Stack;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.cypher.clauses.ClauseEnum.*;

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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(CREATE, "(n:Person)"));
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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH, "(n:Person)"));
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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH, "(n:Person)"));
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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH, "(n)"));
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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH, "(n:Person)"));
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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH, "(n)"));
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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH, "(n)"));
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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH, "(n)"));
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
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(MATCH, "(n)"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(WHERE, "n.name='Selene'"));
        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(SET, "n.name='Kate'"));
//        assertThat(clauseInvocationStack.pop()).isEqualTo(new ClauseInvocation(RETURN, "n"));
        assertThat(clauseInvocationStack).isEmpty();
    }
}
