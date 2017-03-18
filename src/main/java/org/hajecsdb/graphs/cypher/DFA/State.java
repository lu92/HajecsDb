package org.hajecsdb.graphs.cypher.DFA;


import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.Result;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class State {
    private String description;

    public State(String description) {
        this.description = description;
    }

    private List<Transition> incomingTransitionList = new LinkedList<>();
    private List<Transition> outgoingTransitionList = new LinkedList<>();

    public Result invoke(Graph graph, Result result, CommandProcessing commandProcessing) {
        Transition transition = getTransition(commandProcessing.getProcessingCommand());
        System.out.println("performing transition: " + transition);
        result = transition.performAction(graph, result, commandProcessing);
        if (transition.getNextState().getOutgoingTransitionList().isEmpty() || commandProcessing.getProcessingCommand().isEmpty()) {
            System.out.println("end of processing command!");
            return result;
        } else {
            System.out.println("left part of command to perform: '" + commandProcessing.getProcessingCommand() + "'");
            transition.getNextState().invoke(graph, result, commandProcessing);
        }
        return result;
    }

    private Transition getTransition(String command) {
        List<Matched> matchedTransitions = outgoingTransitionList.stream()
                .map(transition -> new Matched(transition, transition.isMatched(command)))
                .filter(Matched::isMatched)
                .collect(Collectors.toList());

        if(matchedTransitions.isEmpty()) {
            throw new IllegalArgumentException("not definitive automaton, any transistion from state (" + description + ") matched!");
        }
        if (matchedTransitions.size() == 1) {
            return matchedTransitions.get(0).getTransition();
        } else
            throw new IllegalArgumentException("not definitive automaton, check state (" + description + ") and its structure of transitions");
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

    private class Matched {
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
}
