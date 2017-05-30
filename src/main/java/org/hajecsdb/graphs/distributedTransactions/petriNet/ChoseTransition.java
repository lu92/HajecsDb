package org.hajecsdb.graphs.distributedTransactions.petriNet;

import java.util.List;

public interface ChoseTransition {
    Transition chose(List<Transition> transitionOptions);
}
