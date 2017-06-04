package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.Data;
import lombok.Getter;
import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;

import java.util.ArrayList;
import java.util.List;

@Data
public class Transition {
    private String description;
    private Job job;
    private List<Arc> inputArcList = new ArrayList<>();
    private List<Arc> outputArcList = new ArrayList<>();
    private @Getter CommunicationProtocol communicationProtocol;
    private List<Long> disabled = new ArrayList<>();

    public Transition(String description, Job job) {
        this.description = description;
        this.job = job;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Transition that = (Transition) o;

        return description.equals(that.description);
    }

    @Override
    public int hashCode() {
        return description.hashCode();
    }
}
