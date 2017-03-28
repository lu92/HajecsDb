package org.hajecsdb.graphs.cypher.clauses.helpers;

import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;

import java.util.Optional;

public class Equation {
    private Optional<Property> argumentA;
    private Optional<Property> argumentB;
    private ArithmeticOperator arithmeticOperator;

    public Equation(Optional<Property> argumentA, ArithmeticOperator arithmeticOperator, Optional<Property> argumentB) {
        this.argumentA = argumentA;
        this.argumentB = argumentB;
        this.arithmeticOperator = arithmeticOperator;
    }

    public boolean isFilled() {
        return argumentA.isPresent() && argumentB.isPresent() && arithmeticOperator != null;
    }

    public boolean isTrue() {

        argumentA.get().getType();

        PropertyType type = argumentB.get().getType();

        switch (type) {
            case LONG:
                return longValues((long) argumentA.get().getValue(), (long) argumentB.get().getValue(), arithmeticOperator);

            case STRING:
                System.out.println("STRING");
                return textValues((String) argumentA.get().getValue(), (String) argumentB.get().getValue());

            default:
                throw new IllegalArgumentException("");
        }
    }

    boolean longValues(long valueA, long valueB, ArithmeticOperator operator) {
        switch (operator) {
            case EQUALS:
                return valueA == valueB;

            case GRATER:
                return valueA > valueB;

            case GREATER_THAN_OR_EQUALS:
                return valueA >= valueB;

            case LOWER:
                return valueA < valueB;

            case LOWER_THAN_OR_EQUAL:
                return valueA <= valueB;

            default:
                throw new IllegalArgumentException("Equation operator invalid!");
        }
    }

    boolean textValues(String valueA, String valueB) {
        return valueA.equals(valueB);
    }
}