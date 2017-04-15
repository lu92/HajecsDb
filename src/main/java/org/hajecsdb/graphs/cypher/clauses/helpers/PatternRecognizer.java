package org.hajecsdb.graphs.cypher.clauses.helpers;

class PatternRecognizer {

    private PatternRecognizer() {
    }

    //      \(([\w]+):?([\w]+)?(\{[\w]+:[\w'.,:]+\})?\)
    private final static String NODE_PATTERN = "\\(([\\w]+):?([\\w]+)?(\\{[\\w]+:[\\w'.,:]+\\})?\\)";

    //      \(([\w]+):?([\w]+)?(\{[\w:' }]+)?\)(--|-\[([\w]+):?([\w]+)?\]->|<-\[([\w]+):?([\w]+)?\]-)\(([\w]+):?([\w]+)?(\{[\w:' }]+)?\)
    private final static String RELATIONSHIP_PATTERN = "\\(([\\w]+):?([\\w]+)?(\\{[\\w:' }]+)?\\)(--|-\\[([\\w]+):?([\\w]+)?\\]->|<-\\[([\\w]+):?([\\w]+)?\\]-)\\(([\\w]+):?([\\w]+)?(\\{[\\w:' }]+)?\\)";

    static boolean isNode(String subQuery) {
        return subQuery.matches(NODE_PATTERN);
    }

    static boolean isRelationship(String subQuery) {
        return subQuery.matches(RELATIONSHIP_PATTERN);
    }

    static PatternEnum getType(String subQuery) {
        if (isNode(subQuery)) {
            return PatternEnum.NODE;
        } else if (isRelationship(subQuery)) {
            return PatternEnum.RELATIONSHIP;
        }
        return PatternEnum.EXPRESSION;
    }
}
