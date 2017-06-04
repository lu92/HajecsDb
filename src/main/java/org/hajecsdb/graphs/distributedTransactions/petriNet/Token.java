package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Token {
    private long distributedTransactionId;
//    private HostAddress coordinatorHostAddress;
//    private List<HostAddress> participantHostAddressList;

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
