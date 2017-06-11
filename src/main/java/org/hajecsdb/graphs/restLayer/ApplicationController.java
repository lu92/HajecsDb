package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.restLayer.config.VoterConfig;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RestController
@Import(ClusterConfiguration.class)
public class ApplicationController {

    @Autowired
    private SessionPool sessionPool;

    @Autowired
    private EntityConverter entityConverter;

    @Autowired
    private CypherExecutor cypherExecutor;

    @Autowired
    private Environment environment;

    @Autowired(required = false)
    private VoterConfig voterConfig;

    private TransactionManager transactionManager = new TransactionManager();

    private RestCommunicationProtocol restCommunicationProtocol = new RestCommunicationProtocol();

    private AbstractCluster cluster;

    @RequestMapping(method = RequestMethod.GET, path = "/Session")
    @ResponseBody
    public SessionDto createSession() {
        Session session = sessionPool.createSession();
        return new SessionDto(session.getSessionId());
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/Session")
    @ResponseBody
    public String closeSession(@RequestBody SessionDto sessionDto) {
        return sessionPool.closeSession(sessionDto.getSessionId());
    }

    @RequestMapping(method = RequestMethod.POST, path = "/Cypher")
    @ResponseBody
    public ResultDto execute(@RequestBody Command command) {
        Optional<Session> session = sessionPool.getSession(command.getSessionId());
        if (!session.isPresent()) {
            return createMessage(command.getCommand(), "Session is not opened!");
        } else {

            switch (command.getCommand().toUpperCase()) {
                case "BEGIN":
                    if (session.get().hasOpenedTransaction()) {
                        return createMessage(command.getCommand(), "Transaction is already started!");
                    } else {
                        session.get().setTransactionManager(transactionManager);
                        Transaction transaction = session.get().beginTransaction();
                        ResultRowDto answer = ResultRowDto.builder().contentType(ContentType.LONG).longValue(transaction.getId()).build();
                        ResultDto resultDto = new ResultDto();
                        resultDto.setCommand("BEGIN");
                        Map<Integer, ResultRowDto> content = new HashMap<>();
                        content.put(0, answer);
                        resultDto.setContent(content);
                        return resultDto;
                    }

                case "COMMIT":
                    if (session.get().hasOpenedTransaction()) {
                        cypherExecutor.getTransactionalGraphService().context(session.get().getTransaction()).commit();
                        session.get().clearTransaction();
                        return createMessage(command.getCommand(), "Transaction has been committed!");
                    } else {
                        return createMessage(command.getCommand(), "Transaction is not started!");
                    }

                case "ROLLBACK":
                    if (session.get().getTransaction().isPerformed()) {
                        return createMessage(command.getCommand(), "Transaction is already preformed!");
                    }
                    return null;

                default:
                    Result result = cypherExecutor.execute(session.get().getTransaction(), command.getCommand());
                    return entityConverter.toResult(result);
            }
        }
    }

    @RequestMapping(method = RequestMethod.POST, path = "/3pc/sent")
    @ResponseBody
    public void sendMessage(@RequestBody Message message) {
        restCommunicationProtocol.sendMessage(message);
    }

    @RequestMapping(method = RequestMethod.GET, path = "/clear")
    @ResponseBody
    public String clearPetriNet() {
        cluster.clearPetriNet();
        return "Petri Net is clear!";
    }

    @RequestMapping(method = RequestMethod.POST, path = "/abortTransaction")
    @ResponseBody
    public String abortTransactionByParticipant(@RequestBody AbortTransactionDto abortTransaction) {
        cluster.abortDistributedTransaction(abortTransaction.getDistributedTransactionId(), abortTransaction.isAbort());
        return "Distributed Transaction [" + abortTransaction.getDistributedTransactionId() + "] will be aborted by Participant[" + getPort() + "]";
    }


    @RequestMapping(method = RequestMethod.POST, path = "/3pc/receive")
    @ResponseBody
    public void messageReceived(@RequestBody Message message) {
        initCluster();
        cluster.receiveMessage(message);
    }

    @RequestMapping(method = RequestMethod.POST, path = "/distributedTransaction")
    @ResponseBody
    public void exec(@RequestBody DistributedTransactionCommand command) {
        initCluster();
        cluster.exec(command);
    }

    @RequestMapping(method = RequestMethod.POST, path = "/script")
    @ResponseBody
    public ResultDto execScript(@RequestBody Script script) {
        initCluster();
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


    private ResultDto createMessage(String command, String message) {
        ResultRowDto answer = ResultRowDto.builder().contentType(ContentType.STRING).message(message).build();
        ResultDto resultDto = new ResultDto();
        resultDto.setCommand(command);
        Map<Integer, ResultRowDto> content = new HashMap<>();
        content.put(0, answer);
        resultDto.setContent(content);
        return resultDto;
    }

    private void initCluster() {
        if (cluster == null) {
            if (environment.getActiveProfiles()[0].equals("coordinator")) {
                cluster = new CoordinatorCluster(new HostAddress(getLocalAdress(), getPort()), voterConfig.getHosts(), restCommunicationProtocol, cypherExecutor);
            } else {
                cluster = new ParticipantCluster(new HostAddress(getLocalAdress(), getPort()), voterConfig.getHosts().get(0), restCommunicationProtocol, cypherExecutor);
            }
        }
    }

    private String getLocalAdress() {
        return "127.0.0.1";
    }

    private int getPort() {
        String property = environment.getProperty("local.server.port");
        return Integer.valueOf(property);
    }

}
