package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClausesSeparator;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.distributedTransactions.*;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.restLayer.config.VoterConfig;
import org.hajecsdb.graphs.restLayer.dto.Command;
import org.hajecsdb.graphs.restLayer.dto.DistributedTransactionCommand;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.hajecsdb.graphs.restLayer.dto.SessionDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

@Component
@Profile("coordinator")
public class CoordinatorCluster extends AbstractCluster {
    private Coordinator coordinator;
    private Participant participant;
    private ClausesSeparator clausesSeparator;


    @Autowired
    public CoordinatorCluster(CommunicationProtocol communicationProtocol, CypherExecutor cypherExecutor, VoterConfig voterConfig, Environment environment) {
        super(communicationProtocol, environment);
        this.clausesSeparator = new ClausesSeparator();
        petriNet = create3pcPetriNet();
        List<HostAddress> actualParticipantList = getParticipantHostAddresses(hostAddress, voterConfig.getHosts());
        int numberOfParticipantsOfDistributedTransaction = actualParticipantList.size();

        coordinator = new Coordinator(petriNet, communicationProtocol, hostAddress, numberOfParticipantsOfDistributedTransaction);
        participant = new Participant(petriNet, communicationProtocol, hostAddress, hostAddress, this.cypherExecutor, sessionPool, transactionManager);

        petriNet.setCoordinatorHostAddress(coordinator.getHostAddress());
        petriNet.setParticipantList(actualParticipantList);
    }

    private List<HostAddress> getParticipantHostAddresses(HostAddress hostAddress, List<HostAddress> participantHostAddressList) {
        List<HostAddress> actualParticipantList = new LinkedList<>();
        actualParticipantList.addAll(participantHostAddressList);
        if (!actualParticipantList.contains(hostAddress)) {
            actualParticipantList.add(hostAddress);
        }
        return actualParticipantList;
    }

    @Override
    public void receiveMessage(Message message) {
        Token token = new Token(message.getDistributedTransactionId(), message.getCommand());
        switch (getTargetVoterOfSignal(message.getSignal())) {
            case COORDINATOR:
                coordinator.receiveMessage(message);
                petriNet.fireTransitionsInCoordinatorFlow(token);
                break;

            case PARTICIPANT:
                participant.receiveMessage(message);
                petriNet.fireTransitionsInParticipantFlow(token);
                break;
        }
    }

    @Override
    public ResultDto exec(DistributedTransactionCommand distributedTransactionCommand) {
        Stack<ClauseInvocation> clauseInvocations = clausesSeparator.splitByClauses(distributedTransactionCommand.getCommand());
        if (clauseInvocations.peek().getClause() == ClauseEnum.CREATE_NODE
                || clauseInvocations.peek().getClause() == ClauseEnum.CREATE_RELATIONSHIP) {
            return createMessage(distributedTransactionCommand.getCommand(), "CREATE clauses are not supported!");
        }


        Token token = new Token(distributedTransactionCommand.getDistributedTransactionId(), distributedTransactionCommand.getCommand());
        petriNet.pushInCoordinatorFlow(token);
        petriNet.fireTransitionsInCoordinatorFlow(token);
        return coordinator.getResultOfDistributedTransaction(distributedTransactionCommand.getDistributedTransactionId());
    }

    private VoterType getTargetVoterOfSignal(Signal signal) {
        return signal.getTriggeredVoter().getOppositeType();
    }

    @Override
    public void abortDistributedTransaction(long distributedTransactionId, boolean decision) {
        this.participant.abortDistributedTransaction(distributedTransactionId, decision);
    }

    @Override
    public HostAddress getHostAddress() {
        return hostAddress;
    }

    @Override
    public void clearPetriNet() {
        petriNet.getPlaces().stream().forEach(place -> place.getTokenList().clear());
        this.coordinator.getReceivedMessages().clear();
    }

    @Override
    public ResultDto perform(Command command) {
        return participant.perform(command);
    }

    @Override
    public SessionDto createSession() {
        Session session = sessionPool.createSession();
        return new SessionDto(session.getSessionId());
    }

    @Override
    public String closeSession(String sessionId) {
        return sessionPool.closeSession(sessionId);
    }
}
