package HajecsDb.cypher;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.cypher.CypherDfaBuilder;
import org.hajecsdb.graphs.cypher.DFA.CommandProcessing;
import org.hajecsdb.graphs.cypher.DFA.DFA;
import org.hajecsdb.graphs.cypher.Query;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.*;
import static org.hajecsdb.graphs.cypher.CommandType.CREATE_NODE;

@RunWith(MockitoJUnitRunner.class)
public class CypherDfaBuilderTest {

    @Test
    public void createNodeClauseWithOnlyLabelTest() {

        // given
        String command = "CREATE (n: Person)";
        CypherDfaBuilder cypherDfaBuilder = new CypherDfaBuilder();
        cypherDfaBuilder.buildCreateNodeClause();
        DFA cypherDfa = cypherDfaBuilder.getDfa();

        // when
        List<Query> queries = cypherDfa.parse(command);

        // then
        CommandProcessing commandProcessing = cypherDfa.getCommandProcessing();
        assertThat(commandProcessing.getOriginCommand()).isEqualTo(command);
        assertThat(commandProcessing.getProcessingCommand()).isEmpty();
        assertThat(commandProcessing.getParts()).hasSize(2);
        assertThat(commandProcessing.getParts().get(0).getProceedPart()).isEqualTo("CREATE ");
        assertThat(commandProcessing.getParts().get(0).getState()).isNotNull();
        assertThat(commandProcessing.getParts().get(1).getProceedPart()).isEqualTo("(n: Person)");
        assertThat(commandProcessing.getParts().get(1).getState()).isNotNull();
        assertThat(queries).hasSize(1);
        assertThat(queries.get(0).getCommandType()).isEqualTo(CREATE_NODE);
        assertThat(queries.get(0).getPartOfQuery()).isEqualTo("(n: Person)");
        assertThat(queries.get(0).getVariableName()).isEqualTo("n");
        assertThat(queries.get(0).getLabel()).isEqualTo(new Label("Person"));
        assertThat(queries.get(0).getParameters()).isEmpty();
    }

    @Test
    public void createNodeClauseWithLabelAndParametersTest() {

        // given
        String command = "CREATE (n: Person {name: 'Henry', age: 25, salary: 3000.0})";
        CypherDfaBuilder cypherDfaBuilder = new CypherDfaBuilder();
        cypherDfaBuilder.buildCreateNodeClause();
        DFA cypherDfa = cypherDfaBuilder.getDfa();

        // when
        List<Query> queries = cypherDfa.parse(command);

        // then
        CommandProcessing commandProcessing = cypherDfa.getCommandProcessing();
        assertThat(commandProcessing.getOriginCommand()).isEqualTo(command);
        assertThat(commandProcessing.getProcessingCommand()).isEmpty();
        assertThat(commandProcessing.getParts()).hasSize(2);
        assertThat(commandProcessing.getParts().get(0).getProceedPart()).isEqualTo("CREATE ");
        assertThat(commandProcessing.getParts().get(0).getState()).isNotNull();
        assertThat(commandProcessing.getParts().get(1).getProceedPart()).isEqualTo("(n: Person {name: 'Henry', age: 25, salary: 3000.0})");
        assertThat(commandProcessing.getParts().get(1).getState()).isNotNull();
        assertThat(queries).hasSize(1);
        assertThat(queries.get(0).getCommandType()).isEqualTo(CREATE_NODE);
        assertThat(queries.get(0).getPartOfQuery()).isEqualTo("(n: Person {name: 'Henry', age: 25, salary: 3000.0})");
        assertThat(queries.get(0).getVariableName()).isEqualTo("n");
        assertThat(queries.get(0).getLabel()).isEqualTo(new Label("Person"));

        List<Property> parameters = queries.get(0).getParameters();
        assertThat(parameters).hasSize(3);
        assertThat(parameters.get(0)).isEqualTo(new Property("name", STRING, "Henry"));
        assertThat(parameters.get(1)).isEqualTo(new Property("age", INT, 25));
        assertThat(parameters.get(2)).isEqualTo(new Property("salary", DOUBLE, 3000.00));
    }

}
