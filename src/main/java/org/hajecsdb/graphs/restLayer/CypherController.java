package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.cypher.CypherExecutor;
import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.restLayer.dto.Command;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
class CypherController {

    private CypherExecutor cypherExecutor = new CypherExecutor();
    private GraphImpl graph = new GraphImpl("/home", "test");
    private InternalBinaryGraphOperationScheduler internalBinaryGraphOperationScheduler = new InternalBinaryGraphOperationScheduler();

    @RequestMapping(method = RequestMethod.POST, path = "/Cypher")
    @ResponseBody
    public Result execute(@RequestBody Command command) {
        Result result = cypherExecutor.execute(graph, command.getCommand());
        internalBinaryGraphOperationScheduler.add(command.getCommand(), result);
        return result;
    }

    public Result execute (List<String> commands) {
        return null;
    }
}
