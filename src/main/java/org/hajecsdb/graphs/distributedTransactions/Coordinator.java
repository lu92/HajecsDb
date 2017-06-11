package org.hajecsdb.graphs.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Place;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.hajecsdb.graphs.distributedTransactions.Signal.*;

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
        long distributedTransactionId = message.getDistributedTransactionId();
        System.out.println(LocalDateTime.now() + "\t" + hostAddress + " received: " + message);
        receivedMessages.add(message);

        switch(message.getSignal()) {

            case VOTE_COMMIT:
                if (isWaitingState(distributedTransactionId)) {
                    System.out.println("RECEIVED VOTE COMMIT");
                    if (eachParticipantHasVotedCommitOrAbort(distributedTransactionId)) {
                        System.out.println("ALL PARTICIPANTS VOTED");
                        Place P1_wait = petriNet.getPlace("P1-WAIT").get();
                        P1_wait.getTokenList().add(new Token(distributedTransactionId));
                        if (allParticipantsAreReady(distributedTransactionId)) {
                            // disable T4 transition
                            P1_wait.disableTransition(distributedTransactionId, "T4");
                        } else {
                            // disable T3 transition
                            P1_wait.disableTransition(distributedTransactionId, "T3");
                        }

                        // remove from receivedMessage all messages which has received transaction id
                        // and VOTE_COMMIT OR VOTE ABORT SIGNAL

                        List<Message> messagesToDelete = receivedMessages.stream()
                                .filter(receivedMessage -> receivedMessage.getDistributedTransactionId() == distributedTransactionId
                                        && (receivedMessage.getSignal() == VOTE_COMMIT || receivedMessage.getSignal() == VOTE_ABORT))
                                .collect(Collectors.toList());

                        receivedMessages.removeAll(messagesToDelete);
                    }
                }
                break;

            case VOTE_ABORT:
                if (isWaitingState(distributedTransactionId)) {
                    System.out.println("RECEIVED VOTE ABORT");
                    rememberParticipantWhoAbortedTransaction(distributedTransactionId, message.getSourceHostAddress());
                    if (eachParticipantHasVotedCommitOrAbort(distributedTransactionId)) {
                        System.out.println("ALL PARTICIPANTS VOTED");
                        Place P1_wait = petriNet.getPlace("P1-WAIT").get();
                        P1_wait.getTokenList().add(new Token(distributedTransactionId));
                        if (allParticipantsAreReady(distributedTransactionId)) {
                            System.out.println("ALL PARTICIPANT ARE READY TO COMMIT!");
                            // disable T4 transition
                            P1_wait.disableTransition(distributedTransactionId, "T4");
                        } else {
                            System.out.println("NOT ALL PARTICIPANT ARE READY TO COMMIT!");
                            // disable T3 transition
                            P1_wait.disableTransition(distributedTransactionId, "T3");
                        }

                        // remove from receivedMessage all messages which has received transaction id
                        // and VOTE_COMMIT OR VOTE ABORT SIGNAL

                        List<Message> messagesToDelete = receivedMessages.stream()
                                .filter(receivedMessage -> receivedMessage.getDistributedTransactionId() == distributedTransactionId
                                        && (receivedMessage.getSignal() == VOTE_COMMIT || receivedMessage.getSignal() == VOTE_ABORT))
                                .collect(Collectors.toList());

                        receivedMessages.removeAll(messagesToDelete);
                    }
                }
                break;

            case READY_TO_COMMIT:
                if (allParticipantsArePreparedToCommit(distributedTransactionId)) {
                    Place P3_pre_commit = petriNet.getPlace("P3-PRE-COMMIT").get();
                    P3_pre_commit.getTokenList().add(new Token(distributedTransactionId));
                    System.out.println("ALL PARTICIPANTS SENDED READY-TO-COMMIT");
                }
                break;

            case ACK:
                if (allParticipantsCommittedTransaction(distributedTransactionId)) {
                    Place P4_commit = petriNet.getPlace("P4-COMMIT").get();
                    P4_commit.getTokenList().add(new Token(distributedTransactionId));
                    System.out.println("DISTRIBUTED TRANSACTION [" + distributedTransactionId + "] HAS BEEN COMMITTED!");
                    deleteMessagesRelatedWithTransaction(distributedTransactionId);
                }
//                deleteMessagesRelatedWithTransaction(distributedTransactionId);
                break;
        }
    }

    private void rememberParticipantWhoAbortedTransaction(long distributedTransactionId, HostAddress participantHostAddress) {
        if (!petriNet.getParticipantsWhichAbortTransaction().containsKey(distributedTransactionId)) {
            petriNet.getParticipantsWhichAbortTransaction().put(distributedTransactionId, new HashSet<>());
        }
        Set<HostAddress> hostAddresses = petriNet.getParticipantsWhichAbortTransaction().get(distributedTransactionId);
        hostAddresses.add(participantHostAddress);
        petriNet.getParticipantsWhichAbortTransaction().put(distributedTransactionId, hostAddresses);
    }

    private boolean allParticipantsArePreparedToCommit(long distributedTransactionId) {
        return receivedMessages.stream()
                .filter(receivedMessage -> receivedMessage.getDistributedTransactionId() == distributedTransactionId && receivedMessage.getSignal() == READY_TO_COMMIT)
                .count() == numberOfParticipantsOfDistributedTransaction;
    }

    private void deleteMessagesRelatedWithTransaction(long distributedTransactionId) {
        // clear messages related with committed distributed transaction
        List<Message> messagesToDelete = getReceivedMessages().stream()
                .filter(receivedMessage -> receivedMessage.getDistributedTransactionId() == distributedTransactionId)
                .collect(Collectors.toList());
        receivedMessages.removeAll(messagesToDelete);
        System.out.println("COORDINATOR HAS REMOVED MESSAGES RELATED WITH [" + distributedTransactionId + "] DISTRIBUTED TRANSACTION");

        petriNet.getPlaces().stream().forEach(place -> {
            List<Token> tokenList = place.getTokenList().stream()
                    .filter(token -> token.getDistributedTransactionId() == distributedTransactionId)
                    .collect(Collectors.toList());
            place.getTokenList().removeAll(tokenList);
        });
    }

    private boolean eachParticipantHasVotedCommitOrAbort(long distributedTransactionId) {
        return getReceivedMessages().stream()
                .filter(receivedMessage -> receivedMessage.getDistributedTransactionId() == distributedTransactionId)
                .filter(receivedMessage -> receivedMessage.getSignal() == VOTE_COMMIT || receivedMessage.getSignal() == VOTE_ABORT)
                .count() == this.numberOfParticipantsOfDistributedTransaction;
    }

    private boolean allParticipantsAreReady(long distributedTransactionId) {
        return receivedMessages.stream()
                .filter(receivedMessage -> receivedMessage.getDistributedTransactionId() == distributedTransactionId)
                .allMatch(receivedMessage -> receivedMessage.getSignal() == VOTE_COMMIT);
    }

    private boolean allParticipantsCommittedTransaction(long distributedTransactionId) {
        return receivedMessages.stream()
                .filter(receivedMessage -> receivedMessage.getDistributedTransactionId() == distributedTransactionId && receivedMessage.getSignal() == ACK)
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
