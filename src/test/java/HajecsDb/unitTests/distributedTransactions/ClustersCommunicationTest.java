package HajecsDb.unitTests.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.Signal;
import org.hajecsdb.graphs.restLayer.CoordinatorCluster;
import org.hajecsdb.graphs.restLayer.ParticipantCluster;
import org.hajecsdb.graphs.restLayer.config.CoordinatorConfig;
import org.hajecsdb.graphs.restLayer.config.ParticipantConfig;
import org.hajecsdb.graphs.restLayer.dto.DistributedTransactionCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.env.Environment;

import java.util.Arrays;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ClustersCommunicationTest {

    @Mock
    private Environment coordinatorEnvironmentMock;

    @Mock
    private Environment participant2EnvironmentMock;

    @Mock
    private Environment participant3EnvironmentMock;

    @Mock
    private CoordinatorConfig coordinatorConfig;

    @Mock
    private ParticipantConfig participantConfig;

    private int distributedTransactionId = 100;
    private String command = "Cyper Query";

    @Before
    public void setup() {
        Mockito.when(coordinatorEnvironmentMock.getProperty("server.port")).thenReturn("7000");
        Mockito.when(participant2EnvironmentMock.getProperty("server.port")).thenReturn("8000");
        Mockito.when(participant3EnvironmentMock.getProperty("server.port")).thenReturn("9000");
    }

    @Test
    public void coordinatorAndParticipantClusterWithCommitTest() {
        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);

        Mockito.when(coordinatorConfig.getHosts()).thenReturn(Arrays.asList(participant1HostAddress, participant2HostAddress));
        Mockito.when(participantConfig.getHosts()).thenReturn(Arrays.asList(coordinatorHostAddress));

        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(mockedCommunicationProtocol, null, coordinatorConfig, coordinatorEnvironmentMock);
        ParticipantCluster participant2Cluster = new ParticipantCluster(mockedCommunicationProtocol, null, participantConfig, participant2EnvironmentMock);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);

        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, command);

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue()).hasSize(12);
        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.ACK),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.ACK)
        );
    }

    @Test
    public void coordinatorAndTwoParticipantClusterEachCommitTest() {

        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);
        HostAddress participant3HostAddress = new HostAddress("127.0.0.1", 9000);

        Mockito.when(coordinatorConfig.getHosts()).thenReturn(Arrays.asList(participant1HostAddress, participant2HostAddress, participant3HostAddress));
        Mockito.when(participantConfig.getHosts()).thenReturn(Arrays.asList(coordinatorHostAddress));

        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(mockedCommunicationProtocol, null, coordinatorConfig, coordinatorEnvironmentMock);
        ParticipantCluster participant2Cluster = new ParticipantCluster(mockedCommunicationProtocol, null, participantConfig, participant2EnvironmentMock);
        ParticipantCluster participant3Cluster = new ParticipantCluster(mockedCommunicationProtocol, null, participantConfig, participant3EnvironmentMock);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);
        mockedCommunicationProtocol.addCluster(participant3Cluster);

        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, command);

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue()).hasSize(18);
        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant3HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, participant3HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant3HostAddress, command, null, Signal.PREPARE_TO_COMMIT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, participant3HostAddress, coordinatorHostAddress, command, null, Signal.READY_TO_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant3HostAddress, command, null, Signal.GLOBAL_COMMIT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.ACK),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.ACK),
                new Message(distributedTransactionId, participant3HostAddress, coordinatorHostAddress, command, null, Signal.ACK)
        );
    }

    @Test
    public void coordinatorAndParticipantClusterParticipant2AbortTest() {
        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);

        Mockito.when(coordinatorConfig.getHosts()).thenReturn(Arrays.asList(participant1HostAddress, participant2HostAddress));
        Mockito.when(participantConfig.getHosts()).thenReturn(Arrays.asList(coordinatorHostAddress));

        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(mockedCommunicationProtocol, null, coordinatorConfig, coordinatorEnvironmentMock);
        ParticipantCluster participant2Cluster = new ParticipantCluster(mockedCommunicationProtocol, null, participantConfig, participant2EnvironmentMock);
        participant2Cluster.abortDistributedTransaction(distributedTransactionId, true);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);

        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, command);

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue().size()).isEqualTo(6);
        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_ABORT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.GLOBAL_ABORT),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.ACK)
        );
    }

    @Test
    public void coordinatorAndParticipantClusterParticipant1AbortTest() {
        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);

        Mockito.when(coordinatorConfig.getHosts()).thenReturn(Arrays.asList(participant1HostAddress, participant2HostAddress));
        Mockito.when(participantConfig.getHosts()).thenReturn(Arrays.asList(coordinatorHostAddress));

        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(mockedCommunicationProtocol, null, coordinatorConfig, coordinatorEnvironmentMock);
        ParticipantCluster participant2Cluster = new ParticipantCluster(mockedCommunicationProtocol, null, participantConfig, participant2EnvironmentMock);
        coordinatorCluster.abortDistributedTransaction(distributedTransactionId, true);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);

        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, command);

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue().size()).isEqualTo(6);
        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_ABORT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_COMMIT),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.GLOBAL_ABORT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.ACK)
        );
    }


    @Test
    public void coordinatorAndTwoParticipantClusterEachAbortTest() {
        MockedCommunicationProtocol mockedCommunicationProtocol = new MockedCommunicationProtocol();

        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant1HostAddress = new HostAddress("127.0.0.1", 7000);
        HostAddress participant2HostAddress = new HostAddress("127.0.0.1", 8000);
        HostAddress participant3HostAddress = new HostAddress("127.0.0.1", 9000);

        Mockito.when(coordinatorConfig.getHosts()).thenReturn(Arrays.asList(participant1HostAddress, participant2HostAddress, participant3HostAddress));
        Mockito.when(participantConfig.getHosts()).thenReturn(Arrays.asList(coordinatorHostAddress));

        CoordinatorCluster coordinatorCluster = new CoordinatorCluster(mockedCommunicationProtocol, null, coordinatorConfig, coordinatorEnvironmentMock);
        ParticipantCluster participant2Cluster = new ParticipantCluster(mockedCommunicationProtocol,null, participantConfig, participant2EnvironmentMock);
        ParticipantCluster participant3Cluster = new ParticipantCluster(mockedCommunicationProtocol,null, participantConfig, participant3EnvironmentMock);

        coordinatorCluster.abortDistributedTransaction(distributedTransactionId, true);
        participant2Cluster.abortDistributedTransaction(distributedTransactionId, true);
        participant3Cluster.abortDistributedTransaction(distributedTransactionId, true);

        mockedCommunicationProtocol.addCluster(coordinatorCluster);
        mockedCommunicationProtocol.addCluster(participant2Cluster);
        mockedCommunicationProtocol.addCluster(participant3Cluster);

        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(distributedTransactionId, command);

        coordinatorCluster.exec(distributedTransactionCommand);

        assertThat(mockedCommunicationProtocol.getMessageQueue()).containsOnly(
                new Message(distributedTransactionId, coordinatorHostAddress, participant1HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant2HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, coordinatorHostAddress, participant3HostAddress, command, null, Signal.PREPARE),
                new Message(distributedTransactionId, participant1HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_ABORT),
                new Message(distributedTransactionId, participant2HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_ABORT),
                new Message(distributedTransactionId, participant3HostAddress, coordinatorHostAddress, command, null, Signal.VOTE_ABORT)
        );
    }

    @Test
    public void participantClusterOnExecShouldThrowExceptionTest() {

        // given
        CommunicationProtocol communicationProtocolMock = Mockito.mock(CommunicationProtocol.class);
        DistributedTransactionCommand distributedTransactionCommand = new DistributedTransactionCommand(100, command);
        HostAddress coordinatorHostAddress = new HostAddress("127.0.0.1", 7000);
        Mockito.when(participantConfig.getHosts()).thenReturn(Arrays.asList(coordinatorHostAddress));

        ParticipantCluster participantCluster = new ParticipantCluster(communicationProtocolMock, null, participantConfig, participant2EnvironmentMock);

        try {
            // when
            participantCluster.exec(distributedTransactionCommand);
        } catch (IllegalStateException e) {
            //then
            assertThat(e.getMessage()).isEqualTo("Participant cannot coordinate distributed transaction!");
        }
    }
}
