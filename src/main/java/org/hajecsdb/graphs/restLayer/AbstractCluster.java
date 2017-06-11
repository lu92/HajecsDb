package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.ThreePhaseCommitPetriNetBuilder;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.restLayer.dto.DistributedTransactionCommand;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;

public abstract class AbstractCluster {

    private HostAddress hostAddress;
    private CommunicationProtocol communicationProtocol;
    protected PetriNet petriNet;

    public AbstractCluster(HostAddress hostAddress, CommunicationProtocol communicationProtocol) {
        this.hostAddress = hostAddress;
        this.communicationProtocol = communicationProtocol;
    }

    public abstract void receiveMessage(Message message);

    public abstract ResultDto exec(DistributedTransactionCommand distributedTransactionCommand);

    public abstract void abortDistributedTransaction(long distributedTransactionId, boolean decision);

    public HostAddress getHostAddress() {
        return hostAddress;
    }

    public PetriNet getPetriNet() {
        return petriNet;
    }

    protected PetriNet create3pcPetriNet() {
        return new ThreePhaseCommitPetriNetBuilder()
                .communicationProtocol(communicationProtocol)
                .sourceHostAddress(hostAddress)
                .build();
    }

    public abstract void clearPetriNet();
}
