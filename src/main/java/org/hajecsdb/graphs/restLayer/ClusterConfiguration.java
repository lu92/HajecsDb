package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.ThreePhaseCommitPetriNetBuilder;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.transactions.TransactionManager;
import org.hajecsdb.graphs.transactions.transactionalGraph.TransactionalGraphService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClusterConfiguration {

    @Bean
    public TransactionManager getTransactionManager() {
        return new TransactionManager();
    }


    @Bean
    public CommunicationProtocol getCommunicationProtocol() {
        return new RestCommunicationProtocol();
    }

    @Bean
    public ThreePhaseCommitPetriNetBuilder getThreePhaseCommitPetriNetBuilder() {
        return new ThreePhaseCommitPetriNetBuilder();
    }

    @Bean
    public PetriNet getThreePhaseCommitPetriNet() {
        System.out.println("Petri Net initialization!");
        PetriNet petriNet = getThreePhaseCommitPetriNetBuilder()
                .communicationProtocol(getCommunicationProtocol())
                .build();
        return petriNet;
    }

    @Bean
    public TransactionalGraphService getTransactionalGraphService() {
        return new TransactionalGraphService();
    }


}
