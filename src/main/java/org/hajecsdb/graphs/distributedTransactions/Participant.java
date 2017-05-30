package org.hajecsdb.graphs.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Place;

import java.util.List;
import java.util.stream.Collectors;

public class Participant extends Voter {

    public Participant(long distributedTransactionId, PetriNet petriNet, CommunicationProtocol communicationProtocol, HostAddress hostAddress) {
        super(distributedTransactionId, petriNet, communicationProtocol, hostAddress);
    }

    public List<Place> getActualPlaces(long distributedTransactionId) {
        return petriNet.getParticipantFlowPlaces().stream()
                .filter(place -> place.getTokenList().stream().anyMatch(token -> token.getDistributedTransactionId() == distributedTransactionId))
                .collect(Collectors.toList());
    }
}
