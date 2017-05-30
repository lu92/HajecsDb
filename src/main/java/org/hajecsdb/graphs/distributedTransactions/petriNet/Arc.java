package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
class Arc {
    private @Getter Place place;
    private @Getter Transition transition;
    private @Getter ArcDirection arcDirection;
    private @Getter int weight;

}
