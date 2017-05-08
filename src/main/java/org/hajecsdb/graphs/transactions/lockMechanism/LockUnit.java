package org.hajecsdb.graphs.transactions.lockMechanism;


import lombok.Data;
import lombok.Setter;
import org.hajecsdb.graphs.core.ResourceType;

@Data
public class LockUnit {
    private final @Setter
    ResourceType resourceType;
    private final @Setter long resourceId;
}
