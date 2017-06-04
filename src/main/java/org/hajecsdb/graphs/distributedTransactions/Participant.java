package org.hajecsdb.graphs.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Place;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class Participant extends Voter {

    private boolean abortDistributedTransaction;

    public Participant(PetriNet petriNet, CommunicationProtocol communicationProtocol, HostAddress hostAddress) {
        super(petriNet, communicationProtocol, hostAddress);
    }

    @Override
    public void sendMessage(Message message) {

    }

    @Override
    public void receiveMessage(Message message) {
        System.out.println(LocalDateTime.now() + "\t" + hostAddress + " received: " + message);

        if (isVotingState(message.getDistributedTransactionId())) {
            Place P5_initial = getActualPlaces(message.getDistributedTransactionId()).get(0);
            if (abortDistributedTransaction) {

                // disable T2 transition
                P5_initial.disableTransition(message.getDistributedTransactionId(), "T2");
                System.out.println("Distributed Transaction " + message.getDistributedTransactionId() + " aborted by Participant " + hostAddress);
            } else {
                P5_initial.disableTransition(message.getDistributedTransactionId(), "T1");
                System.out.println("Distributed Transaction " + message.getDistributedTransactionId() + " accepted by Participant " + hostAddress);
            }
        }
    }

    public List<Place> getActualPlaces(long distributedTransactionId) {
        return petriNet.getParticipantFlowPlaces().stream()
                .filter(place -> place.getTokenList().stream().anyMatch(token -> token.getDistributedTransactionId() == distributedTransactionId))
                .collect(Collectors.toList());
    }


    public void abortDistributedTransaction(boolean decision) {
        this.abortDistributedTransaction = decision;
    }


    private boolean isVotingState(long distributedTransactionId) {
        List<Place> actualPlaces = getActualPlaces(distributedTransactionId);
        return actualPlaces.size() == 1 && actualPlaces.get(0).getDescription().equalsIgnoreCase("P5-INITIAL");
    }
}
