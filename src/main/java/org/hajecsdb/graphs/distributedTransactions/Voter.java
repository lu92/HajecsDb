package org.hajecsdb.graphs.distributedTransactions;

import lombok.Getter;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;

public abstract class Voter {
    protected PetriNet petriNet;
    protected CommunicationProtocol communicationProtocol;
    protected  @Getter HostAddress hostAddress;

    public Voter(PetriNet petriNet, CommunicationProtocol communicationProtocol, HostAddress hostAddress) {
        this.petriNet = petriNet;
        this.communicationProtocol = communicationProtocol;
        this.hostAddress = hostAddress;
    }

    public abstract void sendMessage(Message message);

    public abstract void receiveMessage(Message message);
}
