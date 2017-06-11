package org.hajecsdb.graphs.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Place;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Participant extends Voter {

    private boolean abortDistributedTransaction;
    private HostAddress coordinatorHostAddress;
    private Map<Long, Boolean> transactionsToAbort = new HashMap<>();

    public Participant(PetriNet petriNet, CommunicationProtocol communicationProtocol, HostAddress hostAddress, HostAddress coordinatorHostAddress) {
        super(petriNet, communicationProtocol, hostAddress);
        this.coordinatorHostAddress = coordinatorHostAddress;
    }

    @Override
    public void sendMessage(Message message) {

    }

    @Override
    public void receiveMessage(Message message) {
        System.out.println(LocalDateTime.now() + "\t" + hostAddress + " received: " + message);
        switch (message.getSignal()) {
            case PREPARE:
                Token token = new Token(message.getDistributedTransactionId());
                petriNet.pushInParticipantFlow(token);
                break;

            case PREPARE_TO_COMMIT:
                Place P7_ready = petriNet.getPlace("P7-READY").get();
                P7_ready.getTokenList().add(new Token(message.getDistributedTransactionId()));
                System.out.println("RECEIVED PREPARE_TO_COMMIT");
                break;

            case GLOBAL_COMMIT:
//                Place P7_ready_2 = petriNet.getPlace("P7-READY").get();
//                P7_ready_2.getTokenList().add(new Token(message.getDistributedTransactionId()));
                Place P7_ready_2 = petriNet.getPlace("P8-PRE-COMMIT").get();
                P7_ready_2.getTokenList().add(new Token(message.getDistributedTransactionId()));
                System.out.println("RECEIVED GLOBAL_COMMIT");

//                petriNet.getPlaces().stream().forEach(place -> {
//                    List<Token> tokenList = place.getTokenList().stream()
//                            .filter(token2 -> token2.getDistributedTransactionId() == message.getDistributedTransactionId())
//                            .collect(Collectors.toList());
//                    place.getTokenList().removeAll(tokenList);
//                });

                break;
        }

        if (isVotingState(message.getDistributedTransactionId())) {
            Place P5_initial = getActualPlaces(message.getDistributedTransactionId()).get(0);
            if (isTransactionAborted(message.getDistributedTransactionId())) {

                // disable T2 transition
                P5_initial.disableTransition(message.getDistributedTransactionId(), "T2");
                System.out.println("Distributed Transaction " + message.getDistributedTransactionId() + " aborted by Participant " + hostAddress);
            } else {
                P5_initial.disableTransition(message.getDistributedTransactionId(), "T1");
                System.out.println("Distributed Transaction " + message.getDistributedTransactionId() + " accepted by Participant " + hostAddress);
            }
        }
    }

    private boolean isTransactionAborted(long distributedTransactionId) {
        return transactionsToAbort.containsKey(distributedTransactionId) && transactionsToAbort.get(distributedTransactionId);
    }

    public List<Place> getActualPlaces(long distributedTransactionId) {
        return petriNet.getParticipantFlowPlaces().stream()
                .filter(place -> place.getTokenList().stream().anyMatch(token -> token.getDistributedTransactionId() == distributedTransactionId))
                .collect(Collectors.toList());
    }


    public void abortDistributedTransaction(long distributedTransactionId, boolean decision) {
        transactionsToAbort.put(distributedTransactionId, decision);
        this.abortDistributedTransaction = true;
    }


    private boolean isVotingState(long distributedTransactionId) {
        List<Place> actualPlaces = getActualPlaces(distributedTransactionId);
        return actualPlaces.size() == 1 && actualPlaces.get(0).getDescription().equalsIgnoreCase("P5-INITIAL");
    }
}
