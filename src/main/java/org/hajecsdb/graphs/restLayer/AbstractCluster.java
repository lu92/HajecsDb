package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.ThreePhaseCommitPetriNetBuilder;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractCluster {

    protected HostAddress hostAddress;
    protected CommunicationProtocol communicationProtocol;
    protected PetriNet petriNet;

    protected SessionPool sessionPool;
    protected CypherExecutor cypherExecutor;
    protected TransactionManager transactionManager;

    public AbstractCluster(CommunicationProtocol communicationProtocol, Environment environment) {
        this.communicationProtocol = communicationProtocol;
        this.hostAddress = getHostAddress(environment);
        this.sessionPool = new SessionPool();
        this.transactionManager = new TransactionManager();
        this.cypherExecutor = new CypherExecutor();
    }

    public abstract void receiveMessage(Message message);

    public abstract ResultDto exec(DistributedTransactionCommand distributedTransactionCommand);

    public abstract void abortDistributedTransaction(long distributedTransactionId, boolean decision);

    public abstract HostAddress getHostAddress();

    public PetriNet getPetriNet() {
        return petriNet;
    }

    protected PetriNet create3pcPetriNet() {
        return new ThreePhaseCommitPetriNetBuilder()
                .communicationProtocol(communicationProtocol)
                .sourceHostAddress(getHostAddress())
                .build();
    }

    public abstract void clearPetriNet();
    public abstract ResultDto perform(Command command);

    public abstract SessionDto createSession();

    public abstract String closeSession(String sessionId);

    protected HostAddress getHostAddress(Environment environment) {
        int port = Integer.valueOf(environment.getProperty("server.port"));
        return new HostAddress("127.0.0.1", port);
    }

    protected ResultDto execScript(Script script) {
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
