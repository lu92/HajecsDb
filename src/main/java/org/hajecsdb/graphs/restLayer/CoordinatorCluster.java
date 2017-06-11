package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.distributedTransactions.*;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.restLayer.dto.DistributedTransactionCommand;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;

import java.util.LinkedList;
import java.util.List;

import static org.hajecsdb.graphs.restLayer.VoterType.COORDINATOR;

public class CoordinatorCluster extends AbstractCluster {

//    private PetriNet petriNet;
    private Coordinator coordinator;
    private Participant participant;

    public CoordinatorCluster(HostAddress hostAddress, List<HostAddress> participantHostAddressList, CommunicationProtocol communicationProtocol, int numberOfParticipantsOfDistributedTransaction) {
        super(COORDINATOR, hostAddress, communicationProtocol);
        petriNet = create3pcPetriNet();
        coordinator = new Coordinator(petriNet, communicationProtocol, hostAddress, numberOfParticipantsOfDistributedTransaction);
        participant = new Participant(petriNet, communicationProtocol, hostAddress, hostAddress);


        List<HostAddress> actualParticipantList = new LinkedList<>();
        actualParticipantList.add(hostAddress);
        actualParticipantList.addAll(participantHostAddressList);

        petriNet.setCoordinatorHostAddress(coordinator.getHostAddress());
        petriNet.setParticipantList(actualParticipantList);
    }

    @Override
    public void receiveMessage(Message message) {
        Token token = new Token(message.getDistributedTransactionId());
        switch (getTargetVoterOfSignal(message.getSignal())) {
            case COORDINATOR:
                coordinator.receiveMessage(message);
                petriNet.fireTransitionsInCoordinatorFlow(token);
                break;

            case PARTICIPANT:
                participant.receiveMessage(message);
                petriNet.fireTransitionsInParticipantFlow(token);
                break;
        }
    }

    @Override
    public ResultDto exec(DistributedTransactionCommand distributedTransactionCommand) {
        Token token = new Token(distributedTransactionCommand.getDistributedTransactionId());
        petriNet.pushInCoordinatorFlow(token);
        petriNet.fireTransitionsInCoordinatorFlow(token);
        return null;
    }

    private VoterType getTargetVoterOfSignal(Signal signal) {
        return signal.getTriggeredVoter().getOppositeType();
    }

    @Override
    public void abortDistributedTransaction(long distributedTransactionId, boolean decision) {
        this.participant.abortDistributedTransaction(distributedTransactionId, decision);
    }

    @Override
    public void clearPetriNet() {
        petriNet.getPlaces().stream().forEach(place -> place.getTokenList().clear());
        this.coordinator.getReceivedMessages().clear();
    }
}
