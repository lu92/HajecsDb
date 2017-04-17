package org.hajecsdb.graphs.storage.entities;

import lombok.Data;

@Data
class GraphMetaData {
    private String pathDir;     // 120 bytes
    private String graphName;   // 30 bytes
//    private String creationDateTime;
    private long lastGeneratedId;
    private long numberOfNodes;
    private long numberOfRelationships;
}
