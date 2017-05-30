package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.Data;

@Data
public class InputArc {
    private Place place;
    private Transition transition;
    private int weight;

    public InputArc(Place place, Transition transition, int weight) {
        this.place = place;
        this.transition = transition;
        this.weight = weight;
    }
}
