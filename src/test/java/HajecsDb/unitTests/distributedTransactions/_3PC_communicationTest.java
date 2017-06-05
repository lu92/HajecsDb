package HajecsDb.unitTests.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.ThreePhaseCommitPetriNetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class _3PC_communicationTest {

    private ThreePhaseCommitPetriNetBuilder threePhaseCommitPetriNetBuilder =
            new ThreePhaseCommitPetriNetBuilder();

    @Test
    public void participantAbortedDistributedTransactionTest() {
//        // given
//        long distributedTransactionId = 12345;
//
//        Set<String> activePlaces;
//        MockedCommunicationProtocol communicationProtocol = new MockedCommunicationProtocol();
//
//        PetriNet threePhaseCommitPetriNet = threePhaseCommitPetriNetBuilder
//                .communicationProtocol(communicationProtocol)
//                .build();
//
//        Coordinator coordinator = new Coordinator(threePhaseCommitPetriNet, communicationProtocol, new HostAddress("127.0.0.1", 1001),1);
//        Participant participant = new Participant(threePhaseCommitPetriNet, communicationProtocol, new HostAddress("192.168.1.101", 1002));
//
//        participant.abortDistributedTransaction(true);
//
//        communicationProtocol.addParticipant(coordinator);
//        communicationProtocol.addParticipant(participant);
//
//        threePhaseCommitPetriNet.setCoordinator(coordinator);
//        threePhaseCommitPetriNet.setParticipant(participant);
//
//        Token token = new Token(distributedTransactionId);
//
//        threePhaseCommitPetriNet.pushInCoordinatorFlow(token);
//
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P0-INITIAL");
//        assertThat(threePhaseCommitPetriNet.getPlace("P0-INITIAL").get().getTokenList()).hasSize(1);
//
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P1-WAIT", "P5-INITIAL");
//        assertThat(threePhaseCommitPetriNet.getPlace("P1-WAIT").get().getTokenList()).hasSize(1);
//        assertThat(threePhaseCommitPetriNet.getPlace("P5-INITIAL").get().getTokenList()).hasSize(1);
//
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P1-WAIT", "P6-ABORT");
//        assertThat(threePhaseCommitPetriNet.getPlace("P1-WAIT").get().getTokenList()).hasSize(2);
//        assertThat(threePhaseCommitPetriNet.getPlace("P6-ABORT").get().getTokenList()).hasSize(1);
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P2-ABORT", "P6-ABORT", "P7-READY");
//        assertThat(threePhaseCommitPetriNet.getPlace("P2-ABORT").get().getTokenList()).hasSize(1);
//        assertThat(threePhaseCommitPetriNet.getPlace("P6-ABORT").get().getTokenList()).hasSize(1);
//        assertThat(threePhaseCommitPetriNet.getPlace("P7-READY").get().getTokenList()).hasSize(1);
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P2-ABORT", "P6-ABORT");
//        assertThat(threePhaseCommitPetriNet.getPlace("P2-ABORT").get().getTokenList()).hasSize(2);
//        assertThat(threePhaseCommitPetriNet.getPlace("P6-ABORT").get().getTokenList()).hasSize(2);
    }

    @Test
    public void participantAcceptedDistributedTransactionTest() {
//        // given
//        long distributedTransactionId = 12345;
//
//        Set<String> activePlaces;
//        MockedCommunicationProtocol communicationProtocol = new MockedCommunicationProtocol();
//
//        PetriNet threePhaseCommitPetriNet = threePhaseCommitPetriNetBuilder
//                .communicationProtocol(communicationProtocol)
//                .build();
//
//        Coordinator coordinator = new Coordinator(threePhaseCommitPetriNet, communicationProtocol, new HostAddress("127.0.0.1", 1001),1);
//        Participant participant = new Participant(threePhaseCommitPetriNet, communicationProtocol, new HostAddress("192.168.1.101", 1002));
//
//        communicationProtocol.addParticipant(coordinator);
//        communicationProtocol.addParticipant(participant);
//
//        threePhaseCommitPetriNet.setCoordinator(coordinator);
//        threePhaseCommitPetriNet.setParticipant(participant);
//
//        Token token = new Token(distributedTransactionId);
//
//
//        threePhaseCommitPetriNet.pushInCoordinatorFlow(token);
//
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P0-INITIAL");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P1-WAIT", "P5-INITIAL");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P1-WAIT", "P7-READY");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P3-PRE-COMMIT", "P8-PRE-COMMIT");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P4-COMMIT", "P9-COMMIT");
    }

//    @Ignore
//    public void twoParticipantsOneAbortedDistributedTransactionTest() {
//        // given
//        long distributedTransactionId = 12345;
//
//        Set<String> activePlaces;
//        MockedCommunicationProtocol communicationProtocol = new MockedCommunicationProtocol();
//
//        PetriNet threePhaseCommitPetriNet = threePhaseCommitPetriNetBuilder
//                .communicationProtocol(communicationProtocol)
//                .build();
//
//        Coordinator coordinator = new Coordinator(distributedTransactionId, threePhaseCommitPetriNet, communicationProtocol, new HostAddress("127.0.0.1", 1001), 1);
//        Participant participant1 = new Participant(distributedTransactionId, threePhaseCommitPetriNet, communicationProtocol, new HostAddress("192.168.1.101", 1002));
//        Participant participant2 = new Participant(distributedTransactionId, threePhaseCommitPetriNet, communicationProtocol, new HostAddress("192.168.1.102", 1002));
//
//        participant1.abortDistributedTransaction(true);
//
//        communicationProtocol.addParticipant(coordinator);
//        communicationProtocol.addParticipant(participant1);
//        communicationProtocol.addParticipant(participant2);
//
//        Token token = new Token(distributedTransactionId, coordinator.getHostAddress(), Arrays.asList(participant1.getHostAddress(),participant2.getHostAddress()));
//
//        threePhaseCommitPetriNet.pushInCoordinatorFlow(token);
//
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P0-INITIAL");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P1-WAIT", "P5-INITIAL");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P1-WAIT", "P6-ABORT");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P2-ABORT", "P6-ABORT", "P7-READY");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P2-ABORT", "P6-ABORT");
//    }

//    @Ignore
//    public void twoParticipantsEachAcceptedDistributedTransactionTest() {
//        // given
//        long distributedTransactionId = 12345;
//
//        Set<String> activePlaces;
//        MockedCommunicationProtocol communicationProtocol = new MockedCommunicationProtocol();
//
//        PetriNet threePhaseCommitPetriNet = threePhaseCommitPetriNetBuilder
//                .communicationProtocol(communicationProtocol)
//                .build();
//
//        Coordinator coordinator = new Coordinator(distributedTransactionId, threePhaseCommitPetriNet, communicationProtocol, new HostAddress("127.0.0.1", 1001),2);
//        Participant participant1 = new Participant(distributedTransactionId, threePhaseCommitPetriNet, communicationProtocol, new HostAddress("192.168.1.101", 1002));
//        Participant participant2 = new Participant(distributedTransactionId, threePhaseCommitPetriNet, communicationProtocol, new HostAddress("192.168.1.101", 1002));
//
//        communicationProtocol.addParticipant(coordinator);
//        communicationProtocol.addParticipant(participant1);
//        communicationProtocol.addParticipant(participant2);
//
//        Token token = new Token(distributedTransactionId, coordinator.getHostAddress(), Arrays.asList(participant1.getHostAddress(), participant2.getHostAddress()));
//
//
//        threePhaseCommitPetriNet.pushInCoordinatorFlow(token);
//
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P0-INITIAL");
//        assertThat(threePhaseCommitPetriNet.getPlace("P0-INITIAL").get().getTokenList()).hasSize(1);
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P1-WAIT", "P5-INITIAL");
//        assertThat(threePhaseCommitPetriNet.getPlace("P1-WAIT").get().getTokenList()).hasSize(1);
//        assertThat(threePhaseCommitPetriNet.getPlace("P5-INITIAL").get().getTokenList()).hasSize(2);
//
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P1-WAIT", "P7-READY");
//        Set<Place> activePlaces1 = threePhaseCommitPetriNet.getActivePlaces();
//        assertThat(threePhaseCommitPetriNet.getPlace("P1-WAIT").get().getTokenList()).hasSize(1);
//        assertThat(threePhaseCommitPetriNet.getPlace("P7-READY").get().getTokenList()).hasSize(2);
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P3-PRE-COMMIT", "P8-PRE-COMMIT");
//
//        System.out.println("FIRE");
//        threePhaseCommitPetriNet.fireTransitions(token);
//        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
//        assertThat(activePlaces).containsOnly("P4-COMMIT", "P9-COMMIT");
//    }
}
