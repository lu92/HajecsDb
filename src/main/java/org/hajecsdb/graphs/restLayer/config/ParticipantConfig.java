package org.hajecsdb.graphs.restLayer.config;

import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.restLayer.VoterType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
//@Profile("participant")
@PropertySource("participant.properties")
class ParticipantConfig implements VoterConfig {

    @Value("${coordinator.host}")
    private String coordinatorHost;

    @Value("${coordinator.port}")
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
