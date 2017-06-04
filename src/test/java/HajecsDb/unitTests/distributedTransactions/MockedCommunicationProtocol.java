package HajecsDb.unitTests.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.distributedTransactions.Voter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class MockedCommunicationProtocol implements CommunicationProtocol {

    private List<Voter> participantsOfCommunication = new ArrayList<>();

    public void addParticipant(Voter voter) {
        participantsOfCommunication.add(voter);
    }

    @Override
    public void sendMessage(Message message) {
        System.out.println(LocalDateTime.now() + "\t" + "send message to: " + message.getHostAddress() + "[" + message.getSignal() + "]");
        for (Voter voter : participantsOfCommunication) {
            if (voter.getHostAddress().equals(message.getHostAddress())) {
                voter.receiveMessage(message);
            }
        }
    }

//    @Override
//    public Message receiveMessage() {
//        System.out.println("received message from: ");
//        return null;
//    }
}
