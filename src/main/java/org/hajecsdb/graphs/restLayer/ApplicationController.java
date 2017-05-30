package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.restLayer.dto.Command;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.hajecsdb.graphs.restLayer.dto.ResultRowDto;
import org.hajecsdb.graphs.restLayer.dto.SessionDto;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RestController
public class ApplicationController {

    @Autowired
    private SessionPool sessionPool;

    @Autowired
    private EntityConverter entityConverter;

    @Autowired
    private CypherExecutor cypherExecutor;

//    private GraphImpl graph = new GraphImpl("/home", "test");
    private TransactionalGraphService transactionalGraphService = new TransactionalGraphService();
//    private InternalBinaryGraphOperationScheduler internalBinaryGraphOperationScheduler = new InternalBinaryGraphOperationScheduler();

    private TransactionManager transactionManager = new TransactionManager();

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
        if (command.getCommand().equalsIgnoreCase("BEGIN")) {
            Optional<Session> session = sessionPool.getSession(command.getSessionId());
            if (!session.isPresent()) {
                ResultRowDto answer = ResultRowDto.builder().contentType(ContentType.STRING).message("Session is not opened!").build();
                ResultDto resultDto = new ResultDto();
                resultDto.setCommand("BEGIN");
                Map<Integer, ResultRowDto> content = new HashMap<>();
                content.put(0, answer);
                resultDto.setContent(content);
                return resultDto;
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
        }
        Result result = cypherExecutor.execute(transactionalGraphService, null, command.getCommand());
//        internalBinaryGraphOperationScheduler.add(command.getCommand(), result);
        return entityConverter.toResult(result);
    }

}
