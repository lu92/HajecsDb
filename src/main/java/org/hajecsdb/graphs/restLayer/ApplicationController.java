package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.distributedTransactions.*;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Place;
import org.hajecsdb.graphs.distributedTransactions.petriNet.Token;
import org.hajecsdb.graphs.restLayer.config.VoterConfig;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;

@RestController
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

    @Value("${server.port}")
    private int port;

    private TransactionalGraphService transactionalGraphService = new TransactionalGraphService();

    private TransactionManager transactionManager = new TransactionManager();

    private RestCommunicationProtocol restCommunicationProtocol = new RestCommunicationProtocol();

    private ThreePhaseCommitPetriNetBuilder threePhaseCommitPetriNetBuilder =
            new ThreePhaseCommitPetriNetBuilder();

    private PetriNet threePhaseCommitPetriNet = threePhaseCommitPetriNetBuilder
            .communicationProtocol(restCommunicationProtocol)
            .build();

    private Coordinator coordinator;

    private List<Participant> participantList;

    public ApplicationController() {
        coordinator = new Coordinator(
                threePhaseCommitPetriNet,
                restCommunicationProtocol,
                new HostAddress("127.0.0.1", 7000),
                2);

        Participant participant1 = new Participant(
                threePhaseCommitPetriNet,
                restCommunicationProtocol,
                new HostAddress("127.0.0.1", 8000));

        participant1.abortDistributedTransaction(false);

        Participant participant2 = new Participant(
                threePhaseCommitPetriNet,
                restCommunicationProtocol,
                new HostAddress("127.0.0.1", 9000));

        participant2.abortDistributedTransaction(false);

        participantList = Arrays.asList(participant1, participant2);

        threePhaseCommitPetriNet.setCoordinator(coordinator);
        threePhaseCommitPetriNet.setParticipantList(participantList);
    }

    private Participant getParticipant() {
        return participantList.stream()
                .filter(participant -> participant.getHostAddress().getPort() == port)
                .findFirst().get();
    }

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
                        transactionalGraphService.context(session.get().getTransaction()).commit();
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
                    Result result = cypherExecutor.execute(transactionalGraphService, session.get().getTransaction(), command.getCommand());
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
        threePhaseCommitPetriNet.getPlaces().stream().forEach(place -> place.getTokenList().clear());
        threePhaseCommitPetriNet.getCoordinator().getReceivedMessages().clear();
        return "Petri Net is clear!";
    }

    @RequestMapping(method = RequestMethod.POST, path = "/abortTransaction")
    @ResponseBody
    public String abortTransactionByParticipant(@RequestBody AbortTransactionDto abortTransaction) {
        getParticipant().abortDistributedTransaction(abortTransaction.isAbort());
        return "Distributed Transaction [" + abortTransaction.getDistributedTransactionId() + "] will be aborted by Participant[" + port + "]";
    }


    @RequestMapping(method = RequestMethod.POST, path = "/3pc/receive")
    @ResponseBody
    public void messageReceived(@RequestBody Message message) {

        String profile = environment.getActiveProfiles()[0];

        switch (profile.toUpperCase()) {
            case "COORDINATOR":
                System.out.println(LocalDateTime.now() + "\tCOORDINATOR RECEIVED " + message);
                coordinator.receiveMessage(message);
                threePhaseCommitPetriNet.fireTransitionsInCoordinatorFlow(new Token(message.getDistributedTransactionId()));
                break;

            case "PARTICIPANT":
                System.out.println(LocalDateTime.now() + "\tPARTICIPANT RECEIVED " + message);

                switch (message.getSignal()) {
                    case PREPARE:
                        Token token = new Token(message.getDistributedTransactionId());
                        threePhaseCommitPetriNet.pushInParticipantFlow(token);
                        break;

                    case PREPARE_TO_COMMIT:
                        Place P7_ready = threePhaseCommitPetriNet.getPlace("P7-READY").get();
                        P7_ready.getTokenList().add(new Token(message.getDistributedTransactionId()));
                        System.out.println("RECEIVED PREPARE_TO_COMMIT");
                        break;

                    case GLOBAL_COMMIT:
                        Place P8_pre_commit = threePhaseCommitPetriNet.getPlace("P8-PRE-COMMIT").get();
                        P8_pre_commit.getTokenList().add(new Token(message.getDistributedTransactionId()));
                        System.out.println("RECEIVED GLOBAL_COMMIT");
                        break;
                }

                getParticipant().receiveMessage(message);
                threePhaseCommitPetriNet.fireTransitionsInParticipantFlow(new Token(message.getDistributedTransactionId()));
                break;
        }
    }


    @RequestMapping(method = RequestMethod.POST, path = "/distributedTransaction")
    @ResponseBody
    public void exec(@RequestBody DistributedTransactionCommand command) {
        Token token = new Token(command.getDistributedTransactionId());
        threePhaseCommitPetriNet.pushInCoordinatorFlow(token);
        threePhaseCommitPetriNet.fireTransitionsInCoordinatorFlow(token);
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

}
