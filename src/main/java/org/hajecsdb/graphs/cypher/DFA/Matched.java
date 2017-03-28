package org.hajecsdb.graphs.cypher.DFA;

public class Matched {
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