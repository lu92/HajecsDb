package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.Participant;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.restLayer.config.VoterConfig;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Profile("participant")
public class ParticipantCluster extends AbstractCluster {

    private PetriNet petriNet;
    private Participant participant;

    private SessionPool sessionPool;
    private CypherExecutor cypherExecutor;
    private TransactionManager transactionManager;


    @Autowired
    public ParticipantCluster(CommunicationProtocol communicationProtocol, CypherExecutor cypherExecutor, VoterConfig voterConfig, Environment environment) {
        super(communicationProtocol, environment);
        this.sessionPool = new SessionPool();
        this.transactionManager = new TransactionManager();
        this.cypherExecutor = cypherExecutor;
        petriNet = create3pcPetriNet();
        participant = new Participant(petriNet, communicationProtocol, hostAddress, voterConfig.getHosts().get(0), cypherExecutor, sessionPool, transactionManager);
        petriNet.setCoordinatorHostAddress(voterConfig.getHosts().get(0));
        petriNet.setSourceHostAddress(hostAddress);
    }

    @Override
    public void receiveMessage(Message message) {
        participant.receiveMessage(message);
        Token token = new Token(message.getDistributedTransactionId(), message.getCommand());
        petriNet.fireTransitionsInParticipantFlow(token);
    }

    @Override
    public ResultDto exec(DistributedTransactionCommand distributedTransactionCommand) {
        throw new IllegalStateException("Participant cannot coordinate distributed transaction!");
    }

    @Override
    public void abortDistributedTransaction(long distributedTransactionId, boolean decision) {
        participant.abortDistributedTransaction(distributedTransactionId, decision);
    }

    @Override
    public HostAddress getHostAddress() {
        return hostAddress;
    }

    @Override
    public void clearPetriNet() {
        petriNet.getPlaces().stream().forEach(place -> place.getTokenList().clear());
    }

    @Override
    public ResultDto perform(Command command) {
        return null;
    }

    @Override
    public SessionDto createSession() {
        return null;
    }

    @Override
    public String closeSession(String sessionId) {
        return null;
    }

    @Override
    public ResultDto execScript(Script script) {
        Session session = sessionPool.createSession();
        session.setTransactionManager(transactionManager);
        Transaction transaction = session.beginTransaction();
        for (String command : script.getCommands()) {
            Result result = cypherExecutor.execute(transaction, command);
            if (!result.isCompleted()) {
                cypherExecutor.getTransactionalGraphService().context(transaction).rollback();
                ResultDto rollbackedScript = createMessage(command, "Script has been rollbacked!");
                return rollbackedScript;
            }
        }

        cypherExecutor.getTransactionalGraphService().context(transaction).commit();
        return createMessage("", "Script has been perfomed and committed!");
    }

    protected ResultDto createMessage(String command, String message) {
        ResultRowDto answer = ResultRowDto.builder().contentType(ContentType.STRING).message(message).build();
        ResultDto resultDto = new ResultDto();
        resultDto.setCommand(command);
        Map<Integer, ResultRowDto> content = new HashMap<>();
        content.put(0, answer);
        resultDto.setContent(content);
        return resultDto;
    }
}
