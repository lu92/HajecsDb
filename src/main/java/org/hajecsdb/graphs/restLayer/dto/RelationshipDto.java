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
class RelationshipDto {
    private long id;
    private String label;
    private long beginNodeId;
    private long endNodeId;
    private Direction direction;
    private PropertiesDto propertiesDto;
}
