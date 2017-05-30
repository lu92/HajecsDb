package org.hajecsdb.graphs.distributedTransactions;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Message {
    private long distributedTransactionId;
    private HostAddress hostAddress;
    private Decision decision;
}
