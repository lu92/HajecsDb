package org.hajecsdb.graphs.restLayer.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hajecsdb.graphs.core.PropertyType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public final class PropertyDto {
    private String key;
    private PropertyType type;
    private Object value;
}
