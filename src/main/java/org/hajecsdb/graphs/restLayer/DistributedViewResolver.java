package org.hajecsdb.graphs.restLayer;

import org.hajecsdb.graphs.cypher.clauses.DFA.ClauseInvocation;
import org.hajecsdb.graphs.cypher.clauses.DFA.ClausesSeparator;
import org.hajecsdb.graphs.cypher.clauses.helpers.ContentType;
import org.hajecsdb.graphs.restLayer.dto.ResultDto;
import org.hajecsdb.graphs.restLayer.dto.ResultRowDto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum.*;

public class DistributedViewResolver {
    private ClausesSeparator clausesSeparator = new ClausesSeparator();

    public ResultDto concanate(List<ResultDto> partialResults) {
        String command = getCommand(partialResults);
        Map<Integer, ResultRowDto> resultRowMap = getIntegerResultRowDtoMap(partialResults);
        Stack<ClauseInvocation> clauseInvocations = clausesSeparator.splitByClauses(command);

        if (isDeleteNodeClause(clauseInvocations)) {
            return handleDeleteNodeQuery(command, resultRowMap);
        }

        if (isSetPropertyOfNodeClause(clauseInvocations)) {
            return handleSetPropertyToNode(command, resultRowMap);
        }

        if (isRemovePropertyFromNodeClause(clauseInvocations)) {
            return handleRemovePropertyFromNode(command, resultRowMap);
        }

        return new ResultDto(command, resultRowMap);
    }

    private ResultDto handleRemovePropertyFromNode(String command, Map<Integer, ResultRowDto> resultRowMap) {
        int numberOfSetProperties = resultRowMap.values().stream()
                .map(ResultRowDto::getMessage)
                .mapToInt(message -> Integer.valueOf(message.substring(message.indexOf(": ")+2)))
                .sum();

        String firstMessage = resultRowMap.get(0).getMessage();
        String message = firstMessage.substring(0, firstMessage.indexOf(": ")) + ": " + numberOfSetProperties;
        return createMessage(command, message);
    }

    private boolean isRemovePropertyFromNodeClause(Stack<ClauseInvocation> clauseInvocations) {
        return (clauseInvocations.get(0).getClause() == REMOVE && clauseInvocations.get(1).getClause() == MATCH_NODE) ||
                (clauseInvocations.get(0).getClause() == REMOVE && clauseInvocations.get(1).getClause() == WHERE && clauseInvocations.get(2).getClause() == MATCH_NODE);
    }

    private ResultDto handleSetPropertyToNode(String command, Map<Integer, ResultRowDto> resultRowMap) {
        int numberOfSetProperties = resultRowMap.values().stream()
                .map(ResultRowDto::getMessage)
                .mapToInt(message -> Integer.valueOf(message.substring(16)))
                .sum();
        String message = "Properties set: " + numberOfSetProperties;
        return createMessage(command, message);
    }

    private boolean isSetPropertyOfNodeClause(Stack<ClauseInvocation> clauseInvocations) {
        return (clauseInvocations.get(0).getClause() == SET && clauseInvocations.get(1).getClause() == MATCH_NODE) ||
                (clauseInvocations.get(0).getClause() == SET && clauseInvocations.get(1).getClause() == WHERE && clauseInvocations.get(2).getClause() == MATCH_NODE);
    }

    private Map<Integer, ResultRowDto> getIntegerResultRowDtoMap(List<ResultDto> partialResults) {
        List<ResultRowDto> collectedRows = getResultRowDtos(partialResults);
        return IntStream.range(0, collectedRows.size())
                    .boxed()
                    .collect(Collectors.toMap(i -> i, collectedRows::get));
    }

    private List<ResultRowDto> getResultRowDtos(List<ResultDto> partialResults) {
        return partialResults.stream()
                    .map(resultDto -> resultDto.getContent())
                    .map(integerResultRowDtoMap -> integerResultRowDtoMap.values())
                    .flatMap(resultRowDtos -> resultRowDtos.stream())
                    .collect(Collectors.toList());
    }

    private boolean isDeleteNodeClause(Stack<ClauseInvocation> clauseInvocations) {
        return clauseInvocations.get(0).getClause() == DELETE && clauseInvocations.get(1).getClause() == MATCH_NODE;
    }

    private ResultDto handleDeleteNodeQuery(String command, Map<Integer, ResultRowDto> resultRowMap) {
        int numberOfDeletedNodes = resultRowMap.values().stream()
                .map(ResultRowDto::getMessage)
                .mapToInt(message -> Integer.valueOf(message.substring(15)))
                .sum();
        String message = "Nodes deleted: " + numberOfDeletedNodes;
        return createMessage(command, message);
    }

    private String getCommand(List<ResultDto> partialResults) {
        return partialResults.get(0).getCommand();
    }

    private ResultDto createMessage(String command, String message) {
        ResultRowDto answer = ResultRowDto.builder().contentType(ContentType.STRING).message(message).build();
        ResultDto resultDto = new ResultDto();
        resultDto.setCommand(command);
        Map<Integer, ResultRowDto> content = new HashMap<>();
        content.put(0, answer);
        resultDto.setContent(content);
        return resultDto;
    }
}
