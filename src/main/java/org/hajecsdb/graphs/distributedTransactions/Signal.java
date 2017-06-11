package org.hajecsdb.graphs.distributedTransactions;

import org.hajecsdb.graphs.restLayer.VoterType;

import static org.hajecsdb.graphs.restLayer.VoterType.COORDINATOR;
import static org.hajecsdb.graphs.restLayer.VoterType.PARTICIPANT;

public enum Signal {

    // Coordinator's possible decisions
    PREPARE(COORDINATOR),
    GLOBAL_ABORT(COORDINATOR),
    PREPARE_TO_COMMIT(COORDINATOR),
    GLOBAL_COMMIT(COORDINATOR),

    // Participant's possible decisions
    VOTE_ABORT(PARTICIPANT),
    VOTE_COMMIT(PARTICIPANT),
    READY_TO_COMMIT(PARTICIPANT),
    ACK(PARTICIPANT);

    // type of Voter which triggers concrete singal
    private VoterType triggeredVoter;

    Signal(VoterType triggeredVoter) {
        this.triggeredVoter = triggeredVoter;
    }

    public VoterType getTriggeredVoter() {
        return triggeredVoter;
    }
}
