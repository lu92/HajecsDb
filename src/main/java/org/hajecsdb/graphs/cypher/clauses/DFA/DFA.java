package org.hajecsdb.graphs.cypher.clauses.DFA;

import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;

public class DFA {
    private State beginState;
    private CommandProcessing commandProcessing;

    public DFA() {
        this.beginState = new State(null, "[Begin state]");
    }

    public Result parse(TransactionalGraphService graph, Transaction transaction, CommandProcessing commandProcessing) {
        Result result = new Result();
        result.setCommand(commandProcessing.getOriginCommand());
        result = beginState.invoke(graph, transaction, result, commandProcessing);
        this.commandProcessing = commandProcessing;
        return result;
    }

    public State getBeginState() {
        return beginState;
    }

    public CommandProcessing getCommandProcessing() {
        return commandProcessing;
    }
}
