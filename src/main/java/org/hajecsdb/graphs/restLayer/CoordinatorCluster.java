package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.distributedTransactions.*;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.restLayer.dto.DistributedTransactionCommand;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;

import java.util.LinkedList;
import java.util.List;

public class CoordinatorCluster extends AbstractCluster {
    private Coordinator coordinator;
    private Participant participant;
    private DistributedViewResolver distributedViewResolver;

    public CoordinatorCluster(HostAddress hostAddress, List<HostAddress> participantHostAddressList, CommunicationProtocol communicationProtocol, CypherExecutor cypherExecutor) {
        super(hostAddress, communicationProtocol);
        distributedViewResolver = new DistributedViewResolver();
        petriNet = create3pcPetriNet();
        List<HostAddress> actualParticipantList = getParticipantHostAddresses(hostAddress, participantHostAddressList);
        int numberOfParticipantsOfDistributedTransaction = actualParticipantList.size();

        coordinator = new Coordinator(petriNet, communicationProtocol, hostAddress, numberOfParticipantsOfDistributedTransaction);
        participant = new Participant(petriNet, communicationProtocol, hostAddress, hostAddress, cypherExecutor);

        petriNet.setCoordinatorHostAddress(coordinator.getHostAddress());
        petriNet.setParticipantList(actualParticipantList);
    }

    private List<HostAddress> getParticipantHostAddresses(HostAddress hostAddress, List<HostAddress> participantHostAddressList) {
        List<HostAddress> actualParticipantList = new LinkedList<>();
        actualParticipantList.addAll(participantHostAddressList);
        if (!actualParticipantList.contains(hostAddress)) {
            actualParticipantList.add(hostAddress);
        }
        return actualParticipantList;
    }

    @Override
    public void receiveMessage(Message message) {
        Token token = new Token(message.getDistributedTransactionId(), message.getCommand());
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
        Token token = new Token(distributedTransactionCommand.getDistributedTransactionId(), distributedTransactionCommand.getCommand());
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
