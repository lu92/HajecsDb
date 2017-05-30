package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.Data;
import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class PetriNet {
    private Place beginPlace;
    private Set<Place> places = new HashSet<>();
    private Set<Transition> transitions = new HashSet<>();
    private CommunicationProtocol communicationProtocol;

    private Set<Place> coordinatorFlowPlaces = new HashSet<>();
    private Set<Place> participantFlowPlaces = new HashSet<>();

    public void push(Token token) {
        beginPlace.getTokenList().add(token);
    }

    public Set<String> getNamesOfActivePlaces() {
        return places.stream()
                .filter(place -> place.hasTokens())
                .map(Place::getDescription)
                .collect(Collectors.toSet());
    }

    public Set<Place> getActivePlaces() {
        return places.stream()
                .filter(place -> place.hasTokens())
                .collect(Collectors.toSet());
    }

    public void fireTransitions(Token token) {
        getActivePlaces().stream().forEach(place -> place.fireTransitions(communicationProtocol, token));
    }

    public void choseConcreteTransition(Place place) {
    }

    public Optional<Place> getPlace(String description) {
        return places.stream().filter(place -> place.getDescription().equalsIgnoreCase(description)).findFirst();
    }

    public Optional<Transition> getTransition(String description) {
        return transitions.stream().filter(transition -> transition.getDescription().equalsIgnoreCase(description)).findFirst();
    }
}
