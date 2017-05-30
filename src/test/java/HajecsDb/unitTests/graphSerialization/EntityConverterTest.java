package HajecsDb.unitTests.graphSerialization;

import HajecsDb.integrationTests.Matchers;
import org.fest.assertions.Assertions;
import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.restLayer.EntityConverter;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;
import static org.hajecsdb.graphs.cypher.clauses.helpers.ContentType.NODE;
import static org.hajecsdb.graphs.cypher.clauses.helpers.ContentType.RELATIONSHIP;

@RunWith(MockitoJUnitRunner.class)
public class EntityConverterTest {

    private EntityConverter entityConverter = new EntityConverter();

    private Graph graph;

    public EntityConverterTest() {
        graph = new GraphImpl("test", "test");
        Node node1 = graph.createNode(new Label("Person"),
                new Properties().add(new Property("age", LONG, 25)));
        Node node2 = graph.createNode();
        Node node3 = graph.createNode();
        Node node4 = graph.createNode();
        graph.createRelationship(node1, node2, new Label("CONNECTED"));
        graph.createRelationship(node1, node3, new Label("CONNECTED"));
        graph.createRelationship(node1, node4, new Label("CONNECTED"));
    }

    @Test
    public void mapNodeTest() {
        // given
        NodeDto expectedNode = NodeDto.builder()
                .id(1)
                .label("Person")
                .degree(3)
                .relationships(Arrays.asList(5l, 7l, 9l))   // graph creates two relationships every time!
                .properties(PropertiesDto.builder()
                        .properties(Arrays.asList(
                                new PropertyDto("id", LONG, 1),
                                new PropertyDto("label", STRING, "Person"),
                                new PropertyDto("age", LONG, 25)))
                        .build())
                .build();

        Result result = createResult("Command",
                NODE, graph.getNodeById(1).get());

        // when
        ResultDto resultDto = entityConverter.toResult(result);

        // then
        assertThat(resultDto.getCommand()).isEqualTo("Command");
        assertThat(resultDto.getContent()).hasSize(1);
        assertThat(Matchers.sameAs(resultDto.getContent().get(0).getNode(), expectedNode)).isTrue();
    }

    @Test
    public void mapRelationshipTest() {
        // given
        RelationshipDto relationshipDto = RelationshipDto.builder()
                .id(5)
                .startNodeId(1)
                .endNodeId(2)
                .label("CONNECTED")
                .direction(Direction.OUTGOING)
                .properties(PropertiesDto.builder()
                        .properties(Arrays.asList(
                                new PropertyDto("id", LONG, 1),
                                new PropertyDto("label", STRING, "CONNECTED"),
                                new PropertyDto("startNode", LONG, 1),
                                new PropertyDto("endNode", LONG, 2),
                                new PropertyDto("direction", STRING, Direction.OUTGOING)))
                        .build())
                .build();

        Result result = createResult("Command", RELATIONSHIP, graph.getRelationshipById(5).get());

        // when
        ResultDto resultDto = entityConverter.toResult(result);

        // then
        Assertions.assertThat(Matchers.sameAs(resultDto.getContent().get(0).getRelationship(), relationshipDto)).isTrue();
    }

    @Test
    public void mapStringTest() {
        // given
        Result result = createResult("Command", ContentType.STRING, "Some message");

        // when
        ResultDto resultDto = entityConverter.toResult(result);

        // then
        Assertions.assertThat(Matchers.sameAs(resultDto.getContent().get(0).getMessage(), "Some message")).isTrue();
    }

    @Test
    public void mapIntTest() {
        // given
        Result result = createResult("Command", ContentType.INT, 111);

        // when
        ResultDto resultDto = entityConverter.toResult(result);

        // then
        Assertions.assertThat(Matchers.sameAs(resultDto.getContent().get(0).getIntValue(), 111)).isTrue();
    }

    @Test
    public void mapLongTest() {
        // given
        Result result = createResult("Command", ContentType.LONG, 111l);

        // when
        ResultDto resultDto = entityConverter.toResult(result);

        // then
        Assertions.assertThat(Matchers.sameAs(resultDto.getContent().get(0).getLongValue(), 111l)).isTrue();
    }

    @Test
    public void mapFloatTest() {
        // given
        Result result = createResult("Command", ContentType.FLOAT, 111.0f);

        // when
        ResultDto resultDto = entityConverter.toResult(result);

        // then
        Assertions.assertThat(Matchers.sameAs(resultDto.getContent().get(0).getFloatValue(), 111.0f)).isTrue();
    }

    @Test
    public void mapDoubleTest() {
        // given
        Result result = createResult("Command", ContentType.DOUBLE, 111.00);

        // when
        ResultDto resultDto = entityConverter.toResult(result);

        // then
        Assertions.assertThat(Matchers.sameAs(resultDto.getContent().get(0).getDoubleValue(), 111.00)).isTrue();
    }

    private Result createResult(String command, ContentType type, Object content) {
        Result result = new Result();
        result.setCompleted(true);
        result.setCommand(command);
        ResultRow resultRow = new ResultRow();
        resultRow.setContentType(type);
        switch (type) {
            case NODE:
                resultRow.setNode((Node) content);
                break;

            case RELATIONSHIP:
                resultRow.setRelationship((Relationship) content);
                break;

            case STRING:
                resultRow.setMessage((String) content);
                break;

            case INT:
                resultRow.setIntValue((Integer) content);
                break;

            case LONG:
                resultRow.setLongValue((Long) content);
                break;

            case FLOAT:
                resultRow.setFloatValue((Float) content);
                break;

            case DOUBLE:
                resultRow.setDoubleValue((Double) content);
                break;
        }
        int index = result.getResults().size();
        result.getResults().put(index, resultRow);
        return result;
    }
}
