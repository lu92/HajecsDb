package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.restLayer.dto.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class ApplicationController {

    @Autowired
    private AbstractCluster cluster;

    @RequestMapping(method = RequestMethod.GET, path = "/Session")
    @ResponseBody
    public SessionDto createSession() {
        return cluster.createSession();
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/Session")
    @ResponseBody
    public String closeSession(@RequestBody SessionDto sessionDto) {
        return cluster.closeSession(sessionDto.getSessionId());
    }

    @RequestMapping(method = RequestMethod.POST, path = "/Cypher")
    @ResponseBody
    public ResultDto execute(@RequestBody Command command) {
        return cluster.perform(command);
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
        return "Distributed Transaction [" + abortTransaction.getDistributedTransactionId() + "] will be aborted by Participant[" + cluster.getHostAddress().getPort() + "]";
    }


    @RequestMapping(method = RequestMethod.POST, path = "/3pc/receive")
    @ResponseBody
    public void messageReceived(@RequestBody Message message) {
        cluster.receiveMessage(message);
    }

    @RequestMapping(method = RequestMethod.POST, path = "/distributedTransaction")
    @ResponseBody
    public ResultDto exec(@RequestBody DistributedTransactionBatchScript distributedTransactionBatchScript) {
        return cluster.exec(distributedTransactionBatchScript);
    }

    @RequestMapping(method = RequestMethod.POST, path = "/script")
    @ResponseBody
    public ResultDto execScript(@RequestBody Script script) {
        return cluster.execScript(script);
    }
}
