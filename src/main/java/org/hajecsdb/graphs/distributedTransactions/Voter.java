package org.hajecsdb.graphs.distributedTransactions;

import lombok.Getter;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.distributedTransactions.petriNet.PetriNet;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.hajecsdb.graphs.restLayer.dto.ResultRowDto;

import java.util.HashMap;
import java.util.Map;

public abstract class Voter {
    protected PetriNet petriNet;
    protected CommunicationProtocol communicationProtocol;
    protected  @Getter HostAddress hostAddress;

    public Voter(PetriNet petriNet, CommunicationProtocol communicationProtocol, HostAddress hostAddress) {
        this.petriNet = petriNet;
        this.communicationProtocol = communicationProtocol;
        this.hostAddress = hostAddress;
    }

    public abstract void sendMessage(Message message);

    public abstract void receiveMessage(Message message);

    protected ResultDto createMessage(String command, String message) {
        ResultRowDto answer = ResultRowDto.builder().contentType(ContentType.STRING).message(message).build();
        ResultDto resultDto = new ResultDto();
        resultDto.setCommand(command);
        Map<Integer, ResultRowDto> content = new HashMap<>();
        content.put(0, answer);
        resultDto.setContent(content);
        return resultDto;
    }
}
