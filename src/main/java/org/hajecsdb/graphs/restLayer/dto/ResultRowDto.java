package org.hajecsdb.graphs.restLayer.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public final class ResultRowDto {
    private ContentType contentType;
    private NodeDto node;
}
