package org.hajecsdb.graphs.distributedTransactions.petriNet;

public interface Job {
    void perform(PetriNet petriNet, Token token);
//    Decision getDecission();
}
