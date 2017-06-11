package org.hajecsdb.graphs.restLayer.config;

import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.restLayer.VoterType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Profile("coordinator")
@PropertySource(value = "classpath:coordinator.properties")
public class CoordinatorConfig implements VoterConfig{

    @Value("${participant1.host}")
    private String participant1Host;

    @Value("${participant1.port}")
    private int participant1Port;

    @Value("${participant2.host}")
    private String participant2Host;

    @Value("${participant2.port}")
    private int participant2Port;

    @Value("${participant3.host}")
    private String participant3Host;

    @Value("${participant3.port}")
    private int participant3Port;

    @Override
    public VoterType getVoterRole() {
        return VoterType.COORDINATOR;
    }

    @Override
    public List<HostAddress> getHosts() {
        HostAddress participant1 = new HostAddress(participant1Host, participant1Port);
        HostAddress participant2 = new HostAddress(participant2Host, participant2Port);
        HostAddress participant3 = new HostAddress(participant3Host, participant3Port);

        List<HostAddress> participantList = new ArrayList<>();
        if (!participant1.getHost().isEmpty())
            participantList.add(participant1);

        if (!participant2.getHost().isEmpty())
            participantList.add(participant2);

        if (!participant3.getHost().isEmpty())
            participantList.add(participant3);

        return participantList;
    }
}
