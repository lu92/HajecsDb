package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class Token {
    private long distributedTransactionId;
    private List<String> commands;

    public List<String> getCommands() {
        if (commands == null) {
            commands = new ArrayList<>();
        }
        return commands;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Token token = (Token) o;

        return distributedTransactionId == token.distributedTransactionId;
    }

    @Override
    public int hashCode() {
        return (int) (distributedTransactionId ^ (distributedTransactionId >>> 32));
    }
}
