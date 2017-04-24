package org.hajecsdb.graphs.restLayer.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hajecsdb.graphs.core.Direction;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RelationshipDto {
    private long id;
    private String label;
    private long startNodeId;
    private long endNodeId;
    private Direction direction;
    private PropertiesDto properties;
}
