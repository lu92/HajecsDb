package org.hajecsdb.graphs.distributedTransactions;

public enum Signal {

    // Coordinator's possible decisions
    PREPARE,
    GLOBAL_ABORT,
    PREPARE_TO_COMMIT,
    GLOBAL_COMMIT,

    // Participant's possible decisions
    VOTE_ABORT,
    VOTE_COMMIT,
    READY_TO_COMMIT,
    ACK
}
