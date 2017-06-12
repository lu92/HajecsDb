package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.ThreePhaseCommitPetriNetBuilder;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.springframework.core.env.Environment;

public abstract class AbstractCluster {

    protected HostAddress hostAddress;
    protected CommunicationProtocol communicationProtocol;
    protected PetriNet petriNet;

    public AbstractCluster(CommunicationProtocol communicationProtocol, Environment environment) {
        this.communicationProtocol = communicationProtocol;
        this.hostAddress = getHostAddress(environment);
    }

    public abstract void receiveMessage(Message message);

    public abstract ResultDto exec(DistributedTransactionCommand distributedTransactionCommand);

    public abstract void abortDistributedTransaction(long distributedTransactionId, boolean decision);

    public abstract HostAddress getHostAddress();

    public PetriNet getPetriNet() {
        return petriNet;
    }

    protected PetriNet create3pcPetriNet() {
        return new ThreePhaseCommitPetriNetBuilder()
                .communicationProtocol(communicationProtocol)
                .sourceHostAddress(getHostAddress())
                .build();
    }

    public abstract void clearPetriNet();
    public abstract ResultDto perform(Command command);

    public abstract SessionDto createSession();

    public abstract String closeSession(String sessionId);

    public abstract ResultDto execScript(Script script);

    protected HostAddress getHostAddress(Environment environment) {
        int port = Integer.valueOf(environment.getProperty("server.port"));
        return new HostAddress("127.0.0.1", port);
    }
}
