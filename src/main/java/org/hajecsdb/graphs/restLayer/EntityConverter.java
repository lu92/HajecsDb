package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.ResultRow;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.stream.Collectors;

@Component
public final class EntityConverter {

    public ResultDto toResult(Result result) {
        return ResultDto.builder()
                .command(result.getCommand())
                .content(result.getResults().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> mapEntry(entry))))
                .build();
    }

    private ResultRowDto mapEntry(Map.Entry<Integer, ResultRow> resultRowEntry) {
        switch (resultRowEntry.getValue().getContentType()) {
            case NODE:
                return ResultRowDto.builder().contentType(ContentType.NODE).node(toNode(resultRowEntry.getValue().getNode()))
                        .build();

            case RELATIONSHIP:
                return ResultRowDto.builder().contentType(ContentType.RELATIONSHIP).relationship(toRelationship(resultRowEntry.getValue().getRelationship()))
                        .build();


            case STRING:
                return ResultRowDto.builder().contentType(ContentType.STRING).message(resultRowEntry.getValue().getMessage())
                        .build();

            case INT:
                return ResultRowDto.builder().contentType(ContentType.INT).intValue(resultRowEntry.getValue().getIntValue())
                        .build();

            case LONG:
                return ResultRowDto.builder().contentType(ContentType.LONG).longValue(resultRowEntry.getValue().getLongValue())
                        .build();

            case FLOAT:
                return ResultRowDto.builder().contentType(ContentType.FLOAT).floatValue(resultRowEntry.getValue().getFloatValue())
                        .build();

            case DOUBLE:
                return ResultRowDto.builder().contentType(ContentType.DOUBLE).doubleValue(resultRowEntry.getValue().getDoubleValue())
                        .build();

            default:
                throw new IllegalArgumentException("Content type not recognized!");
        }
    }

    private NodeDto toNode(Node node) {
        return new NodeDto().builder()
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
    }

    private RelationshipDto toRelationship(Relationship relationship) {
        return RelationshipDto.builder()
                .id(relationship.getId())
                .startNodeId(relationship.getStartNode().getId())
                .endNodeId(relationship.getEndNode().getId())
                .label(relationship.getLabel().getName())
                .direction(relationship.getDirection())
                .properties(PropertiesDto.builder().properties(relationship.getAllProperties().getAllProperties().stream()
                        .map(property -> new PropertyDto(property.getKey(), property.getType(), property.getValue()))
                        .collect(Collectors.toList())).build())
                .build();
    }

}
