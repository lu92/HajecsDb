package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.Data;

@Data
public class OutputArc {
    private Transition transition;
    private Place place;
    private int weight;

    public OutputArc(Transition transition, Place place, int weight) {
        this.transition = transition;
        this.place = place;
        this.weight = weight;
    }
}
