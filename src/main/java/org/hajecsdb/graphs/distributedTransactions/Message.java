package org.hajecsdb.graphs.distributedTransactions;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class Message {
    private long distributedTransactionId;
    private HostAddress sourceHostAddress;
    private HostAddress hostAddress;
    private List<String> commands;
    private ResultDto resultDto;
    private Signal signal;

    public List<String> getCommands() {
        if (commands == null) {
            commands = new ArrayList<>();
        }
        return commands;
    }
}
