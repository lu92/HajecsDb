package HajecsDb.integrationTests;

import org.assertj.core.util.Lists;
import org.hajecsdb.graphs.restLayer.dto.NodeDto;
import org.hajecsdb.graphs.restLayer.dto.PropertiesDto;
import org.hajecsdb.graphs.restLayer.dto.PropertyDto;

import java.util.Arrays;

import static org.hajecsdb.graphs.core.PropertyType.LONG;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

class TestSpecification {

    static NodeDto expectedNode1 = NodeDto.builder()
            .id(1)
            .label("Person")
            .degree(0)
            .relationships(Lists.emptyList())
            .properties(PropertiesDto.builder()
                    .properties(Arrays.asList(
                            new PropertyDto("id", LONG, 1),
                            new PropertyDto("label", STRING, "Person")))
                    .build())
            .build();

    static NodeDto expectedNode2 = NodeDto.builder()
            .id(1)
            .label("Person")
            .degree(0)
            .relationships(Lists.emptyList())
            .properties(PropertiesDto.builder()
                    .properties(Arrays.asList(
                            new PropertyDto("id", LONG, 2),
                            new PropertyDto("label", STRING, "Person")))
                    .build())
            .build();

    static NodeDto expectedNode3 = NodeDto.builder()
            .id(1)
            .label("Person")
            .degree(0)
            .relationships(Lists.emptyList())
            .properties(PropertiesDto.builder()
                    .properties(Arrays.asList(
                            new PropertyDto("id", LONG, 3),
                            new PropertyDto("label", STRING, "Person")))
                    .build())
            .build();


}
