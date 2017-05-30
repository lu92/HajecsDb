package org.hajecsdb.graphs.distributedTransactions;

import lombok.Getter;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;

import java.time.LocalDateTime;

public abstract class Voter {
    protected long distributedTransactionId;
    protected PetriNet petriNet;
    protected CommunicationProtocol communicationProtocol;
    protected  @Getter HostAddress hostAddress;
    protected LocalDateTime localDateTime;

    public Voter(long distributedTransactionId, PetriNet petriNet, CommunicationProtocol communicationProtocol, HostAddress hostAddress) {
        this.distributedTransactionId = distributedTransactionId;
        this.petriNet = petriNet;
        this.communicationProtocol = communicationProtocol;
        this.hostAddress = hostAddress;
    }

    public void sendMessage(Message message) {
    }

    public void receiveMessage(Message message) {
        System.out.println(LocalDateTime.now() + "\t" + hostAddress + " received: " + message);
    }
}
