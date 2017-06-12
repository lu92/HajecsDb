package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.distributedTransactions.CommunicationProtocol;
import org.hajecsdb.graphs.distributedTransactions.Message;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@Component
public class RestCommunicationProtocol implements CommunicationProtocol {

    private RestTemplate restTemplate = new RestTemplate();

    @Override
    public void sendMessage(Message message) {
        System.out.println(LocalDateTime.now() + "\t" + "send message to: " + message.getHostAddress() + "[" + message.getSignal() + "]");

        String url = "http://" + message.getHostAddress().getHost() + ":" + message.getHostAddress().getPort() + "/3pc/receive";
        restTemplate.postForObject(url, message, Message.class);
    }
}
