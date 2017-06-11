package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.Data;
import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.HostAddress;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class PetriNet {
    private Place beginPlace;
    private Set<Place> places = new HashSet<>();
    private Set<Transition> transitions = new HashSet<>();
    private CommunicationProtocol communicationProtocol;

    private HostAddress coordinatorHostAddress;
    private List<HostAddress> participantList = new ArrayList<>();

    private Set<Place> coordinatorFlowPlaces = new HashSet<>();
    private Set<Place> participantFlowPlaces = new HashSet<>();

    private Map<Long, Set<HostAddress>> participantsWhichAbortTransaction = new HashMap<>();
    private HostAddress sourceHostAddress;

    public void pushInCoordinatorFlow(Token token) {
        beginPlace.getTokenList().add(token);
    }

    public void pushInParticipantFlow(Token token) {
        Place participantInitPlace = getPlace("P5-INITIAL").get();
        if (!participantInitPlace.getTokenList().contains(token)) {
            participantInitPlace.getTokenList().add(token);
        }
    }

    public Set<Place> getActivePlaces() {
        return places.stream()
                .filter(place -> place.hasTokens())
                .collect(Collectors.toSet());
    }

    public void fireTransitionsInCoordinatorFlow(Token token) {
        getActivePlaces().stream()
                .filter(place -> coordinatorFlowPlaces.contains(place))
                .forEach(place -> place.fireTransitions(this, token));
    }

    public void fireTransitionsInParticipantFlow(Token token) {
        getActivePlaces().stream()
                .filter(place -> participantFlowPlaces.contains(place))
                .forEach(place -> place.fireTransitions(this, token));
    }

    public Optional<Place> getPlace(String description) {
        return places.stream().filter(place -> place.getDescription().equalsIgnoreCase(description)).findFirst();
    }
}
