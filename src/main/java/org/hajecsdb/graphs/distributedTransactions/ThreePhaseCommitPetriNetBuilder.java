package org.hajecsdb.graphs.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.petriNet.*;

import java.util.List;

import static org.hajecsdb.graphs.distributedTransactions.Signal.*;
import static org.hajecsdb.graphs.restLayer.VoterType.COORDINATOR;
import static org.hajecsdb.graphs.restLayer.VoterType.PARTICIPANT;

public class ThreePhaseCommitPetriNetBuilder {

    private CommunicationProtocol communicationProtocol;

    public ThreePhaseCommitPetriNetBuilder communicationProtocol(CommunicationProtocol communicationProtocol) {
        this.communicationProtocol = communicationProtocol;
        return this;
    }

    public PetriNet build() {
        PetriNetBuilder petriNetBuilder = new PetriNetBuilder();

        ChoseTransition defaultChoseTransition = new ChoseTransition() {
            @Override
            public Transition chose(List<Transition> transitionOptions) {
                if (transitionOptions.isEmpty())
                    throw new IllegalArgumentException("There is none choise between transitions!");

                if (transitionOptions.size() == 1) {
                    return transitionOptions.get(0);
                } else {
                    throw new IllegalStateException("Not implemented decision!");
                }
            }
        };

        // Coordinator part
        Place P0_INITIAL = petriNetBuilder.place("P0-INITIAL", COORDINATOR, defaultChoseTransition);
        Place P1_WAIT = petriNetBuilder.place("P1-WAIT", COORDINATOR, new ChoseTransition() {
            @Override
            public Transition chose(List<Transition> transitionOptions) {
                return null;
            }
        });
        Place P2_ABORT = petriNetBuilder.place("P2-ABORT", COORDINATOR);
        Place P3_PRE_COMMIT = petriNetBuilder.place("P3-PRE-COMMIT", COORDINATOR);
        Place P4_COMMIT = petriNetBuilder.place("P4-COMMIT", COORDINATOR);


        petriNetBuilder.addPlaceToCoordinatorFlow(P0_INITIAL);
        petriNetBuilder.addPlaceToCoordinatorFlow(P1_WAIT);
        petriNetBuilder.addPlaceToCoordinatorFlow(P2_ABORT);
        petriNetBuilder.addPlaceToCoordinatorFlow(P3_PRE_COMMIT);
        petriNetBuilder.addPlaceToCoordinatorFlow(P4_COMMIT);

        Transition T0 = petriNetBuilder.transition("T0", (petriNet, token) -> {
            System.out.println("T0 1) write to log (begin_commit)");
            System.out.println("T0 2) message to Cohorts (prepare)");

            petriNet.getParticipantList().stream().forEach(participant -> {
                Message voteRQ = new Message(token.getDistributedTransactionId(), participant.getHostAddress(), PREPARE);
                petriNet.getCoordinator().sendMessage(voteRQ);
            });


//            token.getParticipantHostAddressList().stream().forEach(participantHostAddress -> {
//                Message voteRQ = new Message(token.getDistributedTransactionId(), participantHostAddress, PREPARE);
//                communicationProtocol.sendMessage(voteRQ);
//            });
        });

        Transition T4 = petriNetBuilder.transition("T4", (petriNet, token) -> {
            System.out.println("T4 1) write to log (abort)");
            System.out.println("T4 2)  message to Cohorts (global-abort)");

            petriNet.getParticipantList().stream().forEach(participant -> {
                Message voteRQ = new Message(token.getDistributedTransactionId(), participant.getHostAddress(), GLOBAL_ABORT);
                communicationProtocol.sendMessage(voteRQ);
            });

//            token.getParticipantHostAddressList().stream().forEach(participantHostAddress -> {
//                Message voteRQ = new Message(token.getDistributedTransactionId(), participantHostAddress, GLOBAL_ABORT);
//                communicationProtocol.sendMessage(voteRQ);
//            });
        });

        Transition T3 = petriNetBuilder.transition("T3", (petriNet, token) -> {
            System.out.println("T3 1) write to log (prepare-to-commit)");
            System.out.println("T3 2) message to Cohorts (prepare-to-commit)");

            petriNet.getParticipantList().stream().forEach(participant -> {
                Message voteRQ = new Message(token.getDistributedTransactionId(), participant.getHostAddress(), PREPARE_TO_COMMIT);
                communicationProtocol.sendMessage(voteRQ);
            });

//            token.getParticipantHostAddressList().stream().forEach(participantHostAddress -> {
//                Message voteRQ = new Message(token.getDistributedTransactionId(), participantHostAddress, PREPARE_TO_COMMIT);
//                communicationProtocol.sendMessage(voteRQ);
//            });
        });

        Transition T7 = petriNetBuilder.transition("T7", (petriNet, token) -> {
            System.out.println("T7 1) write to log (commit)");
            System.out.println("T7 2) message to Cohorts (global-commit)");

            petriNet.getParticipantList().stream().forEach(participant -> {
                Message voteRQ = new Message(token.getDistributedTransactionId(), participant.getHostAddress(), GLOBAL_COMMIT);
                communicationProtocol.sendMessage(voteRQ);
            });

//            token.getParticipantHostAddressList().stream().forEach(participantHostAddress -> {
//                Message voteRQ = new Message(token.getDistributedTransactionId(), participantHostAddress, GLOBAL_COMMIT);
//                communicationProtocol.sendMessage(voteRQ);
//            });
        });


        // Participant part
        Place P5_INITIAL = petriNetBuilder.place("P5-INITIAL", PARTICIPANT, new ChoseTransition() {
            @Override
            public Transition chose(List<Transition> transitionOptions) {
                return null;
            }
        });
        Place P6_ABORT = petriNetBuilder.place("P6-ABORT", PARTICIPANT);
        Place P7_READY = petriNetBuilder.place("P7-READY", PARTICIPANT);
        Place P8_PRE_COMMIT = petriNetBuilder.place("P8-PRE-COMMIT", PARTICIPANT);
        Place P9_COMMIT = petriNetBuilder.place("P9-COMMIT", PARTICIPANT);


        petriNetBuilder.addPlaceToParticipantFlow(P5_INITIAL);
        petriNetBuilder.addPlaceToParticipantFlow(P6_ABORT);
        petriNetBuilder.addPlaceToParticipantFlow(P7_READY);
        petriNetBuilder.addPlaceToParticipantFlow(P8_PRE_COMMIT);
        petriNetBuilder.addPlaceToParticipantFlow(P9_COMMIT);

        Transition T1 = petriNetBuilder.transition("T1", (petriNet, token) -> {
            System.out.println("T1 1) write to log (abort)");
            System.out.println("T1 2) message to Coordinator (vote-abort)");

            Message voteAbort = new Message(token.getDistributedTransactionId(), petriNet.getCoordinator().getHostAddress(), VOTE_ABORT);
            communicationProtocol.sendMessage(voteAbort);

//            Message voteAbort = new Message(token.getDistributedTransactionId(), token.getCoordinatorHostAddress(), VOTE_ABORT);
//            communicationProtocol.sendMessage(voteAbort);
        });

        Transition T2 = petriNetBuilder.transition("T2", (petriNet, token) -> {
            System.out.println("T2 1) write to log (ready)");
            System.out.println("T2 2) message to Coordinator (vote-commit)");

            Message voteCommit = new Message(token.getDistributedTransactionId(), petriNet.getCoordinator().getHostAddress(), VOTE_COMMIT);
            communicationProtocol.sendMessage(voteCommit);

//            Message voteCommit = new Message(token.getDistributedTransactionId(), token.getCoordinatorHostAddress(), VOTE_COMMIT);
//            communicationProtocol.sendMessage(voteCommit);
        });

        Transition T5 = petriNetBuilder.transition("T5", (petriNet, token) -> {
            System.out.println("T5 1) write to log (abort)");
            System.out.println("T5 2) potwierdzenie do Coordinator");

            Message ack = new Message(token.getDistributedTransactionId(), petriNet.getCoordinator().getHostAddress(), ACK);
            communicationProtocol.sendMessage(ack);

//            Message ack = new Message(token.getDistributedTransactionId(), token.getCoordinatorHostAddress(), ACK);
//            communicationProtocol.sendMessage(ack);
        });

        Transition T6 = petriNetBuilder.transition("T6", (petriNet, token) -> {
            System.out.println("T6 1) write to log (prepare-to-commit)");
            System.out.println("T6 2) message to Coordinator (ready-to-commit)");

            Message readyToCommit = new Message(token.getDistributedTransactionId(), petriNet.getCoordinator().getHostAddress(), READY_TO_COMMIT);
            communicationProtocol.sendMessage(readyToCommit);

//            Message readyToCommit = new Message(token.getDistributedTransactionId(), token.getCoordinatorHostAddress(), READY_TO_COMMIT);
//            communicationProtocol.sendMessage(readyToCommit);
        });

        Transition T8 = petriNetBuilder.transition("T8", (petriNet, token) -> {
            System.out.println("T8 1) write to log (commit)");
            System.out.println("T8 2) potwierdzenie do Coordinator");

            Message ack = new Message(token.getDistributedTransactionId(), petriNet.getCoordinator().getHostAddress(), ACK);
            communicationProtocol.sendMessage(ack);

//            Message ack = new Message(token.getDistributedTransactionId(), token.getCoordinatorHostAddress(), ACK);
//            communicationProtocol.sendMessage(ack);
        });


        // P0 place
        petriNetBuilder.arc(P0_INITIAL, T0, 1);
        petriNetBuilder.arc(T0, P1_WAIT, 1);
        petriNetBuilder.arc(T0, P5_INITIAL, 1);

        // P1 place
        petriNetBuilder.arc(P1_WAIT, T3, 2);  // WARN
        petriNetBuilder.arc(T3, P3_PRE_COMMIT, 1);
        petriNetBuilder.arc(T3, P7_READY, 1);
        petriNetBuilder.arc(P1_WAIT, T4, 2);
        petriNetBuilder.arc(T4, P2_ABORT, 1);
        petriNetBuilder.arc(T4, P7_READY, 1);


        // P2 place - none outgoing arc

        // P3 place
        petriNetBuilder.arc(P3_PRE_COMMIT, T7, 2);
        petriNetBuilder.arc(T7, P4_COMMIT, 1);
        petriNetBuilder.arc(T7, P8_PRE_COMMIT, 1);

        // P4 place - none outgoing arc

        // P5 place
        petriNetBuilder.arc(P5_INITIAL, T1, 1);
        petriNetBuilder.arc(T1, P1_WAIT, 1);
        petriNetBuilder.arc(T1, P6_ABORT, 1);
        petriNetBuilder.arc(P5_INITIAL, T2, 1); // WARN
        petriNetBuilder.arc(T2, P7_READY, 1);
        petriNetBuilder.arc(T2, P1_WAIT, 1);

        // P6 place - none outgoing arc

        // P7 place
        petriNetBuilder.arc(P7_READY, T5, 1);
        petriNetBuilder.arc(T5, P2_ABORT, 1);
        petriNetBuilder.arc(T5, P6_ABORT, 1);
        petriNetBuilder.arc(P7_READY, T6, 2);
        petriNetBuilder.arc(T6, P3_PRE_COMMIT, 1);
        petriNetBuilder.arc(T6, P8_PRE_COMMIT, 1);

        // P8 place
        petriNetBuilder.arc(P8_PRE_COMMIT, T8, 2);
        petriNetBuilder.arc(T8, P4_COMMIT, 1);
        petriNetBuilder.arc(T8, P9_COMMIT, 1);

        // P9 place - none outgoing arc

        petriNetBuilder.setBeginingPlace(P0_INITIAL);
        petriNetBuilder.setCommunicationProtocol(communicationProtocol);

        return petriNetBuilder.get();
    }
}
