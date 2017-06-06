package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.Data;
import org.hajecsdb.graphs.restLayer.VoterType;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class Place {
    private String description;
    private List<Token> tokenList = new ArrayList<>();
    private Set<Arc> inputArcSet = new HashSet<>();
    private Set<Arc> outputArcSet = new HashSet<>();
    private ChoseTransition choseTransition;
    private VoterType voterType;
    public Place(String description, VoterType voterType) {
        this.description = description;
        this.voterType = voterType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Place place = (Place) o;

        return description.equals(place.description);
    }

    @Override
    public int hashCode() {
        return description.hashCode();
    }

    public boolean hasTokens() {
        return !tokenList.isEmpty();
    }

    public void fireTransitions(PetriNet petriNet, Token concreteToken) {

        List<Transition> fireableTransitions = getActiveTransitions();

        Optional<Transition> chosenTransition = chooseTransition(fireableTransitions, concreteToken);

        if (chosenTransition.isPresent()) {
//            chosenTransition.get().getJob().perform(communicationProtocol, concreteToken);
            chosenTransition.get().getOutputArcList().stream()
                    .map(arc -> arc.getPlace()).forEach(nextPlace -> {

                System.out.println("(" + this.description + ")->[" + chosenTransition.get().getDescription() + "]->(" + nextPlace.getDescription() + ") fired");

                if (this.voterType == nextPlace.voterType) {
                    nextPlace.getTokenList().add(concreteToken);
                }
            });
            tokenList.clear();
            chosenTransition.get().getJob().perform(petriNet, concreteToken);
        }
    }

    private synchronized Optional<Transition> chooseTransition(List<Transition> fireableTransitions, Token token) {
        if (fireableTransitions.isEmpty())
            return Optional.empty();
        if (fireableTransitions.size() == 1) {
            return Optional.of(fireableTransitions.get(0));
        } else {

            Set<Transition> disabledTransitions = fireableTransitions.stream()
                    .filter(transition -> transition.getDisabled().contains(token.getDistributedTransactionId()))
                    .collect(Collectors.toSet());

            List<Transition> res = new ArrayList<>();
            res.addAll(fireableTransitions);
            res.removeAll(disabledTransitions);
            if (res.size() > 1)
                throw new IllegalStateException("Not implemented decision!");

            return Optional.of(res.get(0));
        }
    }

    private List<Transition> getActiveTransitions() {
        int numberOfTokens = tokenList.size();
        return outputArcSet.stream()
                .filter(arc -> arc.getArcDirection() == ArcDirection.PLACE_TO_TRANSITION && arc.getWeight() == numberOfTokens)
                .map(Arc::getTransition)
                .collect(Collectors.toList());
    }

    public void disableTransition(long distributedTransactionId, String transitionDescription) {
        Arc outgoingArc = getOutputArcSet().stream()
                .filter(arc -> arc.getTransition().getDescription().equalsIgnoreCase(transitionDescription))
                .findFirst().get();
        outgoingArc.getTransition().getDisabled().add(distributedTransactionId);
        System.out.println(transitionDescription + " is disabled!");
    }
}
