package org.hajecsdb.graphs.restLayer.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public final class PropertiesDto {
    private List<PropertyDto> properties;

    public List<PropertyDto> getProperties() {
        if (properties == null) {
            properties = new ArrayList<>();
        }
        return properties;
    }
}
