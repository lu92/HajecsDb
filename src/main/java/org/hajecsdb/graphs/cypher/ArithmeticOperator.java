package org.hajecsdb.graphs.cypher;

public enum ArithmeticOperator {
    EQUALS("="),
    GRATER(">"),
    GREATER_THAN_OR_EQUALS(">="),
    LOWER("<"),
    LOWER_THAN_OR_EQUAL("<=");

    private String operator;

    ArithmeticOperator(String operator) {
        this.operator = operator;
    }

    public static ArithmeticOperator getOperator(String symbol) {
        for (ArithmeticOperator arithmeticOperator : values()) {
            if (arithmeticOperator.operator.equals(symbol)) {
                return arithmeticOperator;
            }
        }
        return null;
//        throw new IllegalArgumentException("not recognized operator!");
    }
}
