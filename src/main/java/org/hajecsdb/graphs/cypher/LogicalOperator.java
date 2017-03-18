package org.hajecsdb.graphs.cypher;

public enum LogicalOperator {
    AND("AND"),
    OR("OR");

    private String operator;

    LogicalOperator(String operator) {
        this.operator = operator;
    }

    public static LogicalOperator getOperator(String symbol) {
        for (LogicalOperator logicalOperator : values()) {
            if (logicalOperator.operator.equals(symbol)) {
                return logicalOperator;
            }
        }
        return null;
//        throw new IllegalArgumentException("not recognized operator!");
    }
}
