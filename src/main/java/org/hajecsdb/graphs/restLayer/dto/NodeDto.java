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
public final class NodeDto {
    private long id;
    private String label;
    private int degree;
    private List<Long> relationships;
    private PropertiesDto properties;

    public List<Long> getRelationships() {
        if (relationships == null) {
            relationships = new ArrayList<>();
        }
        return relationships;
    }
}
