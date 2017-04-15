package org.hajecsdb.graphs.cypher.clauses.helpers;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hajecsdb.graphs.cypher.clauses.helpers.PatternEnum.*;

public enum ClauseEnum {
    CREATE_NODE("CREATE", NODE),
    CREATE_RELATIONSHIP("CREATE", RELATIONSHIP),
    MATCH_NODE("MATCH", NODE),
    MATCH_RELATIONSHIP("MATCH", RELATIONSHIP),
    WHERE("WHERE", EXPRESSION),
    DELETE("DELETE", EXPRESSION),
    SET("SET", EXPRESSION),
    REMOVE("REMOVE", EXPRESSION),
    RETURN("RETURN", EXPRESSION),;

    private String clauseName;
    private PatternEnum patternEnum;


    ClauseEnum(String clauseName, PatternEnum patternEnum) {
        this.clauseName = clauseName;
        this.patternEnum = patternEnum;
    }

    public static ClauseEnum recognizeClause(String clause, String subQuery) {
//        System.out.println("try: '" + clause +"'\t'" + subQuery + "'");
        PatternEnum matchedPattern = PatternRecognizer.getType(subQuery);
//        System.out.println("PATTERN: " + matchedPattern);
        List<ClauseEnum> matchedClauses = Arrays.asList(values()).stream()
                .filter(clauseEnum -> clauseEnum.clauseName.equals(clause) && clauseEnum.patternEnum == matchedPattern)
                .collect(Collectors.toList());

        if (matchedClauses.size() == 1) {
            return matchedClauses.get(0);
        } else {
//            System.out.println(matchedClauses);
            throw new IllegalArgumentException("Not recognized clause!");
        }
    }
}
