package org.hajecsdb.graphs.distributedTransactions.petriNet;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.restLayer.VoterType;

public class PetriNetBuilder {

    private PetriNet petriNet = new PetriNet();

    public void setBeginingPlace(Place place) {
        petriNet.setBeginPlace(place);
    }

    public void setCommunicationProtocol(CommunicationProtocol communicationProtocol) {
        petriNet.setCommunicationProtocol(communicationProtocol);
    }

    public Place place(String description, VoterType voterType) {
        Place createdPlace = new Place(description, voterType);
        petriNet.getPlaces().add(createdPlace);
        return createdPlace;
    }

    public Place place(String description, VoterType voterType, ChoseTransition choseTransition) {
        Place createdPlace = new Place(description, voterType);
        createdPlace.setChoseTransition(choseTransition);
        petriNet.getPlaces().add(createdPlace);
        return createdPlace;
    }

    public Transition transition(String description, Job job) {
        Transition createdTransition = new Transition(description, job);
        petriNet.getTransitions().add(createdTransition);
        return createdTransition;
    }

    public void arc(Place place, Transition transition, int weight) {
        Arc arc = new Arc(place, transition, ArcDirection.PLACE_TO_TRANSITION, weight);
        place.getOutputArcSet().add(arc);
        transition.getInputArcList().add(arc);
    }

    public void arc(Transition transition, Place place, int weight) {
        Arc arc = new Arc(place, transition, ArcDirection.TRANSITION_TO_PLACE, weight);
        transition.getOutputArcList().add(arc);
        place.getInputArcSet().add(arc);
    }

    public PetriNet get() {
        return petriNet;
    }

    public void addPlaceToCoordinatorFlow(Place place) {
        petriNet.getCoordinatorFlowPlaces().add(place);
    }

    public void addPlaceToParticipantFlow(Place place) {
        petriNet.getParticipantFlowPlaces().add(place);
    }
}
