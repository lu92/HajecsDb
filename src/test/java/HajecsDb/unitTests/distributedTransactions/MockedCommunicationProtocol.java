package HajecsDb.unitTests.distributedTransactions;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.hajecsdb.graphs.restLayer.AbstractCluster;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class MockedCommunicationProtocol implements CommunicationProtocol {

    private List<AbstractCluster> participantsOfCommunication = new ArrayList<>();
    private Queue<Message> messageQueue = new LinkedList<>();


    public void addCluster(AbstractCluster voter) {
        participantsOfCommunication.add(voter);
    }

    public Queue<Message> getMessageQueue() {
        return messageQueue;
    }

    /*
    polls from messageQueue given number of messages
     */
    public List<Message> pollMessages(int number) {
        if (number > messageQueue.size()) {
            System.out.println("message queue content:\n" + messageQueue);
            throw new IllegalStateException("Incorrent number of message queue");
        }

        List list = new LinkedList();
        for (int i=0; i<number; i++)
            list.add(messageQueue.poll());
        return list;
    }

    @Override
    public void sendMessage(Message message) {
        System.out.println(LocalDateTime.now() + "\t" + "send message to: " + message.getHostAddress() + "[" + message.getSignal() + "]");
        messageQueue.add(message);
        for (AbstractCluster voter : participantsOfCommunication) {
            if (voter.getHostAddress().equals(message.getHostAddress())) {
                voter.receiveMessage(message);
            }
        }
    }
}
