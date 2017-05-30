package org.hajecsdb.graphs.distributedTransactions.petriNet;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;

public interface Job {
    void perform(CommunicationProtocol communicationProtocol, Token token);
//    Decision getDecission();
}
