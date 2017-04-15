package org.hajecsdb.graphs.cypher.clauses.helpers.equationResolver;

public class EquationResolver {
    private Equation firstEquation;
    private Equation secondEquation;
    private LogicalOperator logicalOperator;

    public EquationResolver(Equation firstEquation, LogicalOperator logicalOperator, Equation secondEquation) {
        this.firstEquation = firstEquation;
        this.secondEquation = secondEquation;
        this.logicalOperator = logicalOperator;
    }

    public boolean validate() {

        if (logicalOperator == null && !secondEquation.isFilled()) {
            return firstEquation.isTrue();
        } else {
            if (logicalOperator == LogicalOperator.AND) {
                return firstEquation.isTrue() && secondEquation.isTrue();
            }

            if (logicalOperator == LogicalOperator.OR) {
                return firstEquation.isTrue() || secondEquation.isTrue();
            }
        }

        return false;
    }
}