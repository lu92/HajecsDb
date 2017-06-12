package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.Participant;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.restLayer.config.VoterConfig;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@Profile("participant")
public class ParticipantCluster extends AbstractCluster {

    private PetriNet petriNet;
    private Participant participant;


    @Autowired
    public ParticipantCluster(CommunicationProtocol communicationProtocol, CypherExecutor cypherExecutor, VoterConfig voterConfig, Environment environment) {
        super(communicationProtocol, environment);
        petriNet = create3pcPetriNet();
        participant = new Participant(petriNet, communicationProtocol, hostAddress, voterConfig.getHosts().get(0), cypherExecutor, null, null);
        petriNet.setCoordinatorHostAddress(voterConfig.getHosts().get(0));
        petriNet.setSourceHostAddress(hostAddress);
    }

    @Override
    public void receiveMessage(Message message) {
        participant.receiveMessage(message);
        Token token = new Token(message.getDistributedTransactionId(), message.getCommand());
        petriNet.fireTransitionsInParticipantFlow(token);
    }

    @Override
    public ResultDto exec(DistributedTransactionCommand distributedTransactionCommand) {
        throw new IllegalStateException("Participant cannot coordinate distributed transaction!");
    }

    @Override
    public void abortDistributedTransaction(long distributedTransactionId, boolean decision) {
        participant.abortDistributedTransaction(distributedTransactionId, decision);
    }

    @Override
    public HostAddress getHostAddress() {
        return hostAddress;
    }

    @Override
    public void clearPetriNet() {
        petriNet.getPlaces().stream().forEach(place -> place.getTokenList().clear());
    }

    @Override
    public ResultDto perform(Command command) {
        return null;
    }

    @Override
    public SessionDto createSession() {
        return null;
    }

    @Override
    public String closeSession(String sessionId) {
        return null;
    }

    @Override
    public ResultDto execScript(Script script) {
        return null;
    }
}
