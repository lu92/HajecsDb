package org.hajecsdb.graphs.restLayer.config;


import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.restLayer.VoterType;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Profile("coordinator_participant")
@PropertySource(value = "classpath:coordinator.properties")
@PropertySource(value = "classpath:participant.properties")
class CoordinatorAndParticipantConfig implements VoterConfig{

    @Override
    public VoterType getVoterRole() {
        return null;
    }

    @Override
    public List<HostAddress> getHosts() {
        return null;
    }
}
