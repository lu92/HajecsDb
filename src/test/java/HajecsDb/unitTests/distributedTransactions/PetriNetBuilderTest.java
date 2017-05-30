package HajecsDb.unitTests.distributedTransactions;

import org.fest.assertions.Assertions;
import org.hajecsdb.graphs.distributedTransactions.Coordinator;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Participant;
import org.hajecsdb.graphs.distributedTransactions.ThreePhaseCommitPetriNetBuilder;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Place;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Transition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class PetriNetBuilderTest {

    private ThreePhaseCommitPetriNetBuilder threePhaseCommitPetriNetBuilder =
            new ThreePhaseCommitPetriNetBuilder();

    @Test
    public void test() {
        // given
        long distributedTransactionId = 12345;

        Set<String> activePlaces;
        MockedCommunicationProtocol communicationProtocol = new MockedCommunicationProtocol();

        PetriNet threePhaseCommitPetriNet = threePhaseCommitPetriNetBuilder
                .communicationProtocol(communicationProtocol)
                .build();

        Coordinator coordinator = new Coordinator(distributedTransactionId, threePhaseCommitPetriNet, communicationProtocol, new HostAddress("127.0.0.1", 1001));
        Participant participant = new Participant(distributedTransactionId, threePhaseCommitPetriNet, communicationProtocol, new HostAddress("192.168.1.101", 1002));

        communicationProtocol.addParticipant(coordinator);
        communicationProtocol.addParticipant(participant);

        Token token = new Token(distributedTransactionId, coordinator.getHostAddress(), Arrays.asList(participant.getHostAddress()));


        // participant will abort distributed transaction
        Place p5_initial = threePhaseCommitPetriNet.getPlace("P5-INITIAL").get();
        Transition T1 = threePhaseCommitPetriNet.getTransition("T1").get();
        p5_initial.choseTransition(T1);



        threePhaseCommitPetriNet.push(token);

        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
        Assertions.assertThat(activePlaces).containsOnly("P0-INITIAL");

        threePhaseCommitPetriNet.fireTransitions(token);
        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
        Assertions.assertThat(activePlaces).containsOnly("P1-WAIT", "P5-INITIAL");

        threePhaseCommitPetriNet.fireTransitions(token);
        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
        Assertions.assertThat(activePlaces).containsOnly("P1-WAIT", "P6-ABORT");

        threePhaseCommitPetriNet.fireTransitions(token);
        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
        Assertions.assertThat(activePlaces).containsOnly("P2-ABORT", "P6-ABORT", "P7-READY");

        threePhaseCommitPetriNet.fireTransitions(token);
        activePlaces = threePhaseCommitPetriNet.getNamesOfActivePlaces();
        Assertions.assertThat(activePlaces).containsOnly("P2-ABORT", "P6-ABORT");

    }
}
