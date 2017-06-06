package org.hajecsdb.graphs.restLayer.config;

import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.restLayer.VoterType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
//@Profile("coordinator")
@PropertySource("coordinator.properties")
public class CoordinatorConfig implements VoterConfig{

    @Value("${participant1.host}")
    private String participant1Host;

    @Value("${participant1.port}")
    private int participant1Port;

//    @Value("${participant2.host}")
//    private String participant2Host;
//
//    @Value("${participant2.port}")
//    private int participant2Port;

    @Override
    public VoterType getVoterRole() {
        return VoterType.COORDINATOR;
    }

    @Override
    public List<HostAddress> getHosts() {
        HostAddress participant1 = new HostAddress(participant1Host, participant1Port);
//        HostAddress participant2 = new HostAddress(participant2Host, participant2Port);
//        return Arrays.asList(participant1, participant2);
        return Arrays.asList(participant1);
    }
}
