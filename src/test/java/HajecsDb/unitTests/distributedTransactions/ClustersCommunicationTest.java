package HajecsDb.unitTests.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.Signal;
import org.hajecsdb.graphs.restLayer.CoordinatorCluster;
import org.hajecsdb.graphs.restLayer.ParticipantCluster;
import org.hajecsdb.graphs.restLayer.dto.DistributedTransactionCommand;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ClustersCommunicationTest {

    @Test
    public void coordinatorAndParticipantClusterWithCommitTest() {

        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);

        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(coordinatorHostAddress, Arrays.asList(participant2HostAddress), mockedCommunicationProtocol,2);
        ParticipantCluster participant2Cluster = new ParticipantCluster(participant2HostAddress, coordinatorHostAddress, mockedCommunicationProtocol);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);

        int distributedTransactionId = 100;
        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, "Cypher Query");

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.ACK),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.ACK)
        );
    }

    @Test
    public void coordinatorAndTwoParticipantClusterEachCommitTest() {

        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);
        HostAddress participant3HostAddress = new HostAddress("127.0.0.1", 9000);

        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(coordinatorHostAddress, Arrays.asList(participant2HostAddress, participant3HostAddress), mockedCommunicationProtocol,3);
        ParticipantCluster participant2Cluster = new ParticipantCluster(participant2HostAddress, coordinatorHostAddress, mockedCommunicationProtocol);
        ParticipantCluster participant3Cluster = new ParticipantCluster(participant3HostAddress, coordinatorHostAddress, mockedCommunicationProtocol);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);
        mockedCommunicationProtocol.addCluster(participant3Cluster);

        int distributedTransactionId = 100;
        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, "Cypher Query");

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant3HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, participant3HostAddress, coordinatorHostAddress, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant3HostAddress, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, participant3HostAddress, coordinatorHostAddress, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant3HostAddress, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.ACK),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.ACK),
                new Message(distributedTransactionId, participant3HostAddress, coordinatorHostAddress, Signal.ACK)
        );
    }

    @Test
    public void coordinatorAndParticipantClusterParticipant2AbortTest() {

        long distributedTransactionId = 100;
        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);


        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(coordinatorHostAddress, Arrays.asList(participant2HostAddress), mockedCommunicationProtocol,2);
        ParticipantCluster participant2Cluster = new ParticipantCluster(participant2HostAddress, coordinatorHostAddress, mockedCommunicationProtocol);
        participant2Cluster.abortDistributedTransaction(distributedTransactionId, true);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);

        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, "Cypher Query");

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue().size()).isEqualTo(6);
        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.VOTE_ABORT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.GLOBAL_ABORT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.ACK)
        );
    }

    @Test
    public void coordinatorAndParticipantClusterParticipant1AbortTest() {

        long distributedTransactionId = 100;
        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);


        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(coordinatorHostAddress, Arrays.asList(participant2HostAddress), mockedCommunicationProtocol,2);
        ParticipantCluster participant2Cluster = new ParticipantCluster(participant2HostAddress, coordinatorHostAddress, mockedCommunicationProtocol);
        coordinatorCluster.abortDistributedTransaction(distributedTransactionId, true);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);

        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, "Cypher Query");

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue().size()).isEqualTo(6);
        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.VOTE_ABORT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.GLOBAL_ABORT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.ACK)
        );
    }


    @Test
    public void coordinatorAndTwoParticipantClusterEachAbortTest() {

        int distributedTransactionId = 100;
        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);
        HostAddress participant3HostAddress = new HostAddress("127.0.0.1", 9000);

        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(coordinatorHostAddress, Arrays.asList(participant2HostAddress, participant3HostAddress), mockedCommunicationProtocol,3);
        ParticipantCluster participant2Cluster = new ParticipantCluster(participant2HostAddress, coordinatorHostAddress, mockedCommunicationProtocol);
        ParticipantCluster participant3Cluster = new ParticipantCluster(participant3HostAddress, coordinatorHostAddress, mockedCommunicationProtocol);

        coordinatorCluster.abortDistributedTransaction(distributedTransactionId, true);
        participant2Cluster.abortDistributedTransaction(distributedTransactionId, true);
        participant3Cluster.abortDistributedTransaction(distributedTransactionId, true);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);
        mockedCommunicationProtocol.addCluster(participant3Cluster);

        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, "Cypher Query");

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant3HostAddress, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, Signal.VOTE_ABORT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, Signal.VOTE_ABORT),
                new Message(distributedTransactionId, participant3HostAddress, coordinatorHostAddress, Signal.VOTE_ABORT)
        );
    }

    @Test
    public void participantClusterOnExecShouldThrowExceptionTest() {

        // given
        CommunicationProtocol communicationProtocolMock = Mockito.mock(CommunicationProtocol.class);
        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(100, "Cypher Query");
        ParticipantCluster participantCluster = new ParticipantCluster(new HostAddress("127.0.0.1", 8000), null, communicationProtocolMock);

        try {
            // when
            participantCluster.exec(distributedTransactionCommand);
        } catch (IllegalStateException e) {
            //then
            assertThat(e.getMessage()).isEqualTo("Participant cannot coordinate distributed transaction!");
        }
    }
}
