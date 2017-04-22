package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.stream.Collectors;

@Component
final class EntityConverter {

    ResultDto toResult(Result result) {
        ResultDto resultDto = ResultDto.builder()
                .command(result.getCommand())
                .content(result.getResults().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> ResultRowDto.builder().contentType(ContentType.NODE).node(toNode(entry.getValue().getNode()))
                                        .build())))
                .build();
        return resultDto;
    }

    private NodeDto toNode(Node node) {
        NodeDto nodeDto = new NodeDto().builder()
                .id(node.getId())
                .degree(node.getDegree())
                .label(node.getLabel().getName())
                .relationships(node.getRelationships().stream()
                        .map(Relationship::getId)
                        .collect(Collectors.toList()))

                .properties(PropertiesDto.builder().properties(node.getAllProperties().getAllProperties().stream()
                        .map(property -> new PropertyDto(property.getKey(), property.getType(), property.getValue()))
                        .collect(Collectors.toList())).build())
                .build();
        return nodeDto;
    }

}
