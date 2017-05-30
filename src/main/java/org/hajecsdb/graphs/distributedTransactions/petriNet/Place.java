package org.hajecsdb.graphs.distributedTransactions.petriNet;

import lombok.Data;
import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class Place {
    private String description;
    private List<Token> tokenList = new ArrayList<>();
    private Set<Arc> inputArcSet = new HashSet<>();
    private Set<Arc> outputArcSet = new HashSet<>();
    private ChoseTransition choseTransition;

    public Place(String description) {
        this.description = description;
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

    public void fireTransitions(CommunicationProtocol communicationProtocol, Token concreteToken) {

        Set<Transition> fireableTransitions = getActiveTransitions();

        Optional<Transition> chosenTransition = chooseTransition(fireableTransitions, concreteToken);

        if (chosenTransition.isPresent()) {
            chosenTransition.get().getJob().perform(communicationProtocol, concreteToken);
            chosenTransition.get().getOutputArcList().stream()
                    .map(arc -> arc.getPlace()).forEach(nextPlace -> {

                System.out.println("(" + this.description + ")->[" + chosenTransition.get().getDescription() + "]->(" + nextPlace.getDescription() + ") fired");

                nextPlace.getTokenList().add(concreteToken);
//                chosenTransition.get().getCommunicationProtocol().
            });
//            tokenList.remove(concreteToken);
            tokenList.clear();
        }
    }

    private Optional<Transition> chooseTransition(Set<Transition> fireableTransitions, Token token) {
        if (fireableTransitions.isEmpty())
            return Optional.empty();
        if (fireableTransitions.size() == 1) {
            return Optional.of(fireableTransitions.iterator().next());
        } else {
            throw new IllegalStateException("Not implemented decision!");
        }
    }

    private Set<Transition> getActiveTransitions() {
        int numberOfTokens = tokenList.size();
        return outputArcSet.stream()
                .filter(arc -> arc.getArcDirection() == ArcDirection.PLACE_TO_TRANSITION && arc.getWeight() == numberOfTokens)
                .map(Arc::getTransition)
                .collect(Collectors.toSet());
    }

    public void choseTransition(Transition transition) {
        Optional<Arc> arcOptional = this.getOutputArcSet().stream().filter(arc -> !arc.getTransition().equals(transition)).findFirst();
        if (arcOptional.isPresent()) {
            getOutputArcSet().remove(arcOptional.get());
        } else if (outputArcSet.size() > 1)
            throw new IllegalStateException("Cannot find arc to transition[" + transition.getDescription() + "]");
    }
}
