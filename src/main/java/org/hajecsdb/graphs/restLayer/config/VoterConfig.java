package org.hajecsdb.graphs.restLayer.config;

import org.hajecsdb.graphs.distributedTransactions.HostAddress;
import org.hajecsdb.graphs.restLayer.VoterRole;

import java.util.List;

public interface VoterConfig {
    VoterRole getVoterRole();
    List<HostAddress> getHosts();
}
