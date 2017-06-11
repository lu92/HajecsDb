package org.hajecsdb.graphs.distributedTransactions;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;

@Data
@AllArgsConstructor
public class Message {
    private long distributedTransactionId;
    private HostAddress sourceHostAddress;
    private HostAddress hostAddress;
    private String command;
    private ResultDto resultDto;
    private Signal signal;
}
