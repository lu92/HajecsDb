package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.Participant;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.restLayer.dto.DistributedTransactionCommand;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;

public class ParticipantCluster extends AbstractCluster {

    private PetriNet petriNet;
    private Participant participant;

    public ParticipantCluster(HostAddress hostAddress, HostAddress coordinatorHostAddress, CommunicationProtocol communicationProtocol) {
        super(hostAddress, communicationProtocol);
        petriNet = create3pcPetriNet();
        participant = new Participant(petriNet, communicationProtocol, hostAddress, coordinatorHostAddress);
        petriNet.setCoordinatorHostAddress(coordinatorHostAddress);
        petriNet.setSourceHostAddress(hostAddress);
    }

    @Override
    public void receiveMessage(Message message) {
        participant.receiveMessage(message);
        Token token = new Token(message.getDistributedTransactionId());
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
    public void clearPetriNet() {
        petriNet.getPlaces().stream().forEach(place -> place.getTokenList().clear());
    }
}
