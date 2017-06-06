package org.hajecsdb.graphs.restLayer.config;

import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.restLayer.VoterType;

import java.util.List;

public interface VoterConfig {
    VoterType getVoterRole();
    List<HostAddress> getHosts();
}
