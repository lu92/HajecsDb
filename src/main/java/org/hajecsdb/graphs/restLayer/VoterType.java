package org.hajecsdb.graphs.restLayer;

public enum VoterType {
    COORDINATOR,
    PARTICIPANT;

    public VoterType getOppositeType() {
        return this == COORDINATOR ? PARTICIPANT : COORDINATOR;
    }
}
