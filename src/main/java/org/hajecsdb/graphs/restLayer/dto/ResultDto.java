package org.hajecsdb.graphs.restLayer.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResultDto {
    private String command;
    private Map<Integer, ResultRowDto> content;

    public Map<Integer, ResultRowDto> getContent() {
        if (content == null) {
            content = new HashMap<>();
        }
        return content;
    }
}
