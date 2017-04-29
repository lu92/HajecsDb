package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.restLayer.dto.Command;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.hajecsdb.graphs.restLayer.dto.SessionDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class ApplicationController {

    @Autowired
    private SessionPool sessionPool;

    @Autowired
    private EntityConverter entityConverter;

    @Autowired
    private CypherExecutor cypherExecutor;

    private GraphImpl graph = new GraphImpl("/home", "test");
    private InternalBinaryGraphOperationScheduler internalBinaryGraphOperationScheduler = new InternalBinaryGraphOperationScheduler();

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
        Result result = cypherExecutor.execute(graph, command.getCommand());
        internalBinaryGraphOperationScheduler.add(command.getCommand(), result);
        return entityConverter.toResult(result);
    }

}
