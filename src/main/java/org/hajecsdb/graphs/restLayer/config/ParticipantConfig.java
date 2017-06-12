package org.hajecsdb.graphs.restLayer.config;

import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.restLayer.VoterType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
@Profile("participant")
@PropertySource(value = "classpath:participant.properties")
public class ParticipantConfig implements VoterConfig {

    @Value("${coordinatorHostAddress.host}")
    private String coordinatorHost;

    @Value("${coordinatorHostAddress.port}")
    private int coordinatorPort;

    @Override
    public VoterType getVoterRole() {
        return VoterType.PARTICIPANT;
    }

    @Override
    public List<HostAddress> getHosts() {
        return Arrays.asList(new HostAddress(coordinatorHost, coordinatorPort));
    }
}
