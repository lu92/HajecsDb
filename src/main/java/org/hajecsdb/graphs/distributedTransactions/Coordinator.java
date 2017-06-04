package org.hajecsdb.graphs.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Place;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class Coordinator extends Voter {

    private Queue<Message> receivedMessages = new LinkedList<>();
    private int numberOfParticipantsOfDistributedTransaction;

    public Coordinator(PetriNet petriNet, CommunicationProtocol communicationProtocol, HostAddress hostAddress, int numberOfParticipantsOfDistributedTransaction) {
        super(petriNet, communicationProtocol, hostAddress);
        this.numberOfParticipantsOfDistributedTransaction = numberOfParticipantsOfDistributedTransaction;
    }

    public Queue<Message> getReceivedMessages() {
        return receivedMessages;
    }

    @Override
    public void sendMessage(Message message) {
        communicationProtocol.sendMessage(message);
    }

    @Override
    public void receiveMessage(Message message) {
        System.out.println(LocalDateTime.now() + "\t" + hostAddress + " received: " + message);
        receivedMessages.add(message);

        if (isWaitingState(message.getDistributedTransactionId())) {
            System.out.println("WAITING");
            if (receivedMessages.size() == this.numberOfParticipantsOfDistributedTransaction) {
                Place P1_wait = petriNet.getPlace("P1-WAIT").get();
                P1_wait.getTokenList().add(new Token(message.getDistributedTransactionId()));
                if (allParticipantsAreReady()) {
                    // disable T4 transition
                    P1_wait.disableTransition(message.getDistributedTransactionId(), "T4");
                } else {
                    // disable T3 transition
                    P1_wait.disableTransition(message.getDistributedTransactionId(), "T3");
                }
            }
        }
        if (message.getSignal() == Signal.PREPARE_TO_COMMIT) {
            Place P3_pre_commit = petriNet.getPlace("P3-PRE-COMMIT").get();
            P3_pre_commit.getTokenList().add(new Token(message.getDistributedTransactionId()));
        }
        if (message.getSignal() == Signal.READY_TO_COMMIT) {
            Place P3_pre_commit = petriNet.getPlace("P3-PRE-COMMIT").get();
            P3_pre_commit.getTokenList().add(new Token(message.getDistributedTransactionId()));
        }
        if (message.getSignal() == Signal.ACK) {
            receivedMessages.add(message);
            Place P4_commit = petriNet.getPlace("P4-COMMIT").get();
            P4_commit.getTokenList().add(new Token(message.getDistributedTransactionId()));
            if (allParticipantsCommittedTransaction(message)) {
                System.out.println("DISTRIBUTED TRANSACTION [" + message.getDistributedTransactionId() + "] HAS BEEN COMMITTED!");
            }

            // clear messages related with committed distributed transaction
            List<Message> messagesToDelete = getReceivedMessages().stream()
                    .filter(receiveMessage -> receiveMessage.getDistributedTransactionId() == message.getDistributedTransactionId())
                    .collect(Collectors.toList());
            receivedMessages.removeAll(messagesToDelete);
            System.out.println("COORDINATOR HAS REMOVED MESSAGES RELATED WITH [" + message.getDistributedTransactionId() + "] DISTRIBUTED TRANSACTION");
        }
    }

    private boolean allParticipantsAreReady() {
        return receivedMessages.stream().allMatch(receivedMessage -> receivedMessage.getSignal() == Signal.VOTE_COMMIT);
    }

    private boolean allParticipantsCommittedTransaction(Message message) {
        return receivedMessages.stream()
                .filter(receivedMessage -> receivedMessage.getDistributedTransactionId() == message.getDistributedTransactionId() && receivedMessage.getSignal() == Signal.ACK)
                .count() == numberOfParticipantsOfDistributedTransaction;
    }

    private boolean isWaitingState(long distributedTransactionId) {
        List<Place> actualPlaces = getActualPlaces(distributedTransactionId);
        return actualPlaces.size() == 1 && actualPlaces.get(0).getDescription().equalsIgnoreCase("P1-WAIT");
    }

    public List<Place> getActualPlaces(long distributedTransactionId) {
        return petriNet.getCoordinatorFlowPlaces().stream()
                .filter(place -> place.getTokenList().stream().anyMatch(token -> token.getDistributedTransactionId() == distributedTransactionId))
                .collect(Collectors.toList());
    }
}
