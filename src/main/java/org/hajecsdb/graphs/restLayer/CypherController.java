package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.restLayer.dto.Command;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class CypherController {

    private final EntityConverter entityConverter;
    private final CypherExecutor cypherExecutor;

    private GraphImpl graph = new GraphImpl("/home", "test");
    private InternalBinaryGraphOperationScheduler internalBinaryGraphOperationScheduler = new InternalBinaryGraphOperationScheduler();

    @Autowired
    public CypherController(EntityConverter entityConverter, CypherExecutor cypherExecutor) {
        this.entityConverter = entityConverter;
        this.cypherExecutor = cypherExecutor;
    }

    @RequestMapping(method = RequestMethod.POST, path = "/Cypher")
    @ResponseBody
    public ResultDto execute(@RequestBody Command command) {
        Result result = cypherExecutor.execute(graph, command.getCommand());
        internalBinaryGraphOperationScheduler.add(command.getCommand(), result);
        return entityConverter.toResult(result);
    }

    public Result execute (List<String> commands) {
        return null;
    }
}
