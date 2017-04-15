package org.hajecsdb.graphs.cypher.clauses.DFA;

import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;

import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClausesSeparator {

    private String regex = "CREATE|MATCH|WHERE|DELETE|SET|REMOVE|RETURN";

    public Stack<ClauseInvocation> splitByClauses(String command) {
        return splitByClause(command);
    }

    private Stack<ClauseInvocation> splitByClause(String command) {
        List<String> expressionQueryParts = Arrays.asList(command.split(regex));
        List<String> clauses = Arrays.asList(command.split("\\s")).stream()
                .filter(word -> word.matches(regex))
                .collect(Collectors.toList());

        if (clauses.size() == expressionQueryParts.size()-1) {

            Stack<ClauseInvocation> clauseInvocationStack = new Stack<>();

            IntStream.range(0, clauses.size()).forEach(i -> {
                String clause = clauses.get(i);
                String expressionPart = expressionQueryParts.get(i+1).replaceAll("\\s+","");
                ClauseEnum clauseEnum = ClauseEnum.recognizeClause(clause, expressionPart);
                ClauseInvocation clauseInvocation = new ClauseInvocation(clauseEnum, expressionPart);
                clauseInvocationStack.add(0, clauseInvocation);
            });

            return clauseInvocationStack;

        } else {
            throw new IllegalArgumentException("invalid command!");
        }
    }
}
