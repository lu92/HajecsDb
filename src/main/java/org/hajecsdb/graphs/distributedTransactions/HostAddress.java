package org.hajecsdb.graphs.distributedTransactions;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HostAddress {
    private String host;
    private int port;
}
