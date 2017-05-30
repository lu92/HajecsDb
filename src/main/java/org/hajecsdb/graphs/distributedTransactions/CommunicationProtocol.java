package org.hajecsdb.graphs.distributedTransactions;

public interface CommunicationProtocol {
    void sendMessage(Message message);
//    Message receiveMessage();
}
