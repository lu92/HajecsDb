package org.hajecsdb.graphs.distributedTransactions;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Place;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.restLayer.EntityConverter;
import org.hajecsdb.graphs.restLayer.Session;
import org.hajecsdb.graphs.restLayer.SessionPool;
import org.hajecsdb.graphs.restLayer.dto.Command;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.hajecsdb.graphs.restLayer.dto.ResultRowDto;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public class Participant extends Voter {
    private HostAddress coordinatorHostAddress;
    private Map<Long, Boolean> transactionsToAbort = new HashMap<>();
//    private Map<Long, Transaction> openedTransactions = new HashMap<>();
    private CypherExecutor cypherExecutor;
    private EntityConverter entityConverter;
    private SessionPool sessionPool;
    private TransactionManager transactionManager;

    public Participant(PetriNet petriNet, CommunicationProtocol communicationProtocol, HostAddress hostAddress, HostAddress coordinatorHostAddress,
                       CypherExecutor cypherExecutor, SessionPool sessionPool, TransactionManager transactionManager) {
        super(petriNet, communicationProtocol, hostAddress);
        this.coordinatorHostAddress = coordinatorHostAddress;
        this.cypherExecutor = cypherExecutor;
        this.entityConverter = new EntityConverter();
        this.sessionPool = sessionPool;
        this.transactionManager = transactionManager;
    }

    @Override
    public void sendMessage(Message message) {

    }

    @Override
    public void receiveMessage(Message message) {
        System.out.println(LocalDateTime.now() + "\t" + hostAddress + " received: " + message);
        switch (message.getSignal()) {
            case PREPARE:
                Token token = new Token(message.getDistributedTransactionId(), message.getCommand());
                petriNet.pushInParticipantFlow(token);
                break;

            case PREPARE_TO_COMMIT:
                Place P7_ready = petriNet.getPlace("P7-READY").get();
                P7_ready.getTokenList().add(new Token(message.getDistributedTransactionId(), message.getCommand()));
                System.out.println("RECEIVED PREPARE_TO_COMMIT");

//                cypherExecutor.execute(null, message.getCommand());

                break;

            case GLOBAL_COMMIT:
                Place P8_pre_commit = petriNet.getPlace("P8-PRE-COMMIT").get();
                P8_pre_commit.getTokenList().add(new Token(message.getDistributedTransactionId(), message.getCommand()));

                Session session = sessionPool.createSession();
                session.setTransactionManager(transactionManager);
                Transaction transaction = session.beginTransaction();
                Result result = cypherExecutor.execute(transaction, message.getCommand());
                System.out.println("CYPHER OPERATION STATUS [" + message.getCommand() + "]: " + result.isCompleted());
                cypherExecutor.getTransactionalGraphService().context(transaction).commit();
                EntityConverter entityConverter = new EntityConverter();
                ResultDto executedResultDto = entityConverter.toResult(result);
                petriNet.getResultOfLocalPartOfDistributedTransaction().put(message.getDistributedTransactionId(), executedResultDto);
                System.out.println("RECEIVED GLOBAL_COMMIT");
                break;
        }

        if (isVotingState(message.getDistributedTransactionId())) {
            Place P5_initial = getActualPlaces(message.getDistributedTransactionId()).get(0);
            if (isTransactionAborted(message.getDistributedTransactionId())) {

                // disable T2 transition
                P5_initial.disableTransition(message.getDistributedTransactionId(), "T2");
                System.out.println("Distributed Transaction " + message.getDistributedTransactionId() + " aborted by Participant " + hostAddress);
            } else {
                P5_initial.disableTransition(message.getDistributedTransactionId(), "T1");
                System.out.println("Distributed Transaction " + message.getDistributedTransactionId() + " accepted by Participant " + hostAddress);
            }
        }
    }

    private boolean isTransactionAborted(long distributedTransactionId) {
        return transactionsToAbort.containsKey(distributedTransactionId) && transactionsToAbort.get(distributedTransactionId);
    }

    public List<Place> getActualPlaces(long distributedTransactionId) {
        return petriNet.getParticipantFlowPlaces().stream()
                .filter(place -> place.getTokenList().stream().anyMatch(token -> token.getDistributedTransactionId() == distributedTransactionId))
                .collect(Collectors.toList());
    }


    public void abortDistributedTransaction(long distributedTransactionId, boolean decision) {
        transactionsToAbort.put(distributedTransactionId, decision);
    }


    private boolean isVotingState(long distributedTransactionId) {
        List<Place> actualPlaces = getActualPlaces(distributedTransactionId);
        return actualPlaces.size() == 1 && actualPlaces.get(0).getDescription().equalsIgnoreCase("P5-INITIAL");
    }

    public ResultDto perform(Command command) {
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
}
