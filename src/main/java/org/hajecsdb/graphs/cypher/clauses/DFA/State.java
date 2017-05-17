package org.hajecsdb.graphs.cypher.clauses.DFA;


import org.hajecsdb.graphs.cypher.Result;
import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class State {
    private ClauseEnum clauseEnum;
    private String description;
    private List<Transition> incomingTransitionList = new LinkedList<>();
    private List<Transition> outgoingTransitionList = new LinkedList<>();

    public State(String description) {
        this.description = description;
    }

    public State(ClauseEnum clauseEnum, String description) {
        this.clauseEnum = clauseEnum;
        this.description = description;
    }

    public Result invoke(TransactionalGraphService graph, Transaction transaction, Result result, CommandProcessing commandProcessing) {
        ClauseInvocation currentClause = commandProcessing.getClauseInvocationStack().peek();
        Transition transition = getClauseTransition(currentClause);
        result = transition.performAction(graph, transaction, result, commandProcessing);
        if (commandProcessing.getClauseInvocationStack().isEmpty()) {
            System.out.println("end of processing command!");
            return result;
        } else {
            transition.getNextState().invoke(graph, transaction, result, commandProcessing);
        }
        return result;
    }

    private Transition getClauseTransition(ClauseInvocation clauseInvocation) {
        List<Matched> matchedTransitions = outgoingTransitionList.stream()
                .map(transition -> new Matched(transition, transition.isClauseMatched(clauseInvocation)))
                .filter(Matched::isMatched)
                .collect(Collectors.toList());

        if (matchedTransitions.size() == 1) {
            return matchedTransitions.get(0).getTransition();
        } else {
            throw new IllegalArgumentException("Cannot find transition from [" + clauseEnum + "] state. Not matched clause - " + clauseInvocation.getClause());
        }
    }

    class Matched {
        Transition transition;
        boolean matched;

        public Matched(Transition transition, boolean matched) {
            this.transition = transition;
            this.matched = matched;
        }

        public Transition getTransition() {
            return transition;
        }

        public boolean isMatched() {
            return matched;
        }
    }

    public ClauseEnum getClauseEnum() {
        return clauseEnum;
    }

    public String getDescription() {
        return description;
    }

    public List<Transition> getIncomingTransitionList() {
        return incomingTransitionList;
    }

    public List<Transition> getOutgoingTransitionList() {
        return outgoingTransitionList;
    }
}
