package org.hajecsdb.graphs.cypher.clauses.DFA;

import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.Result;

import java.util.function.Predicate;

public class Transition {
    private State beginState;
    private State nextState;
    private Predicate<ClauseInvocation> matchedPredicate;
    private DfaAction action;

    public Transition(State beginState, State nextState, Predicate<ClauseInvocation> matchedPredicate, DfaAction action) {
        this.beginState = beginState;
        this.nextState = nextState;
        this.matchedPredicate = matchedPredicate;
        this.action = action;
        this.beginState.getOutgoingTransitionList().add(this);
        this.nextState.getIncomingTransitionList().add(this);
    }

    public boolean isClauseMatched(ClauseInvocation clauseInvocation) {
        return matchedPredicate.test(clauseInvocation);
    }

    public Result performAction(Graph graph, Result result, CommandProcessing commandProcessing) {
        ClauseInvocation clauseInvocation = commandProcessing.getClauseInvocationStack().peek();
        if (clauseInvocation.getClause() == nextState.getClauseEnum()) {
            System.out.println("[" + nextState.getClauseEnum() + "] clause verified!");
            Result updatedResult = action.perform(graph, result, commandProcessing);
            commandProcessing.getClauseInvocationStack().pop();
            System.out.println("[" + nextState.getClauseEnum() + "] performed!");
            return updatedResult;
        } else  {
            throw new IllegalArgumentException("Invocation stack - internal error!");
        }
    }

    public State getNextState() {
        return nextState;
    }

    @Override
    public String toString() {
        return "Transition{" +
                "beginState=" + beginState.getDescription() +
                ", nextState=" + nextState.getDescription() +
                '}';
    }
}
