package org.hajecsdb.graphs.cypher.clauses.DFA;

import org.hajecsdb.graphs.cypher.clauses.helpers.ClauseEnum;

public class ClauseInvocation {
    private ClauseEnum clause;
    private String subQuery;
    private boolean completed;

    public ClauseInvocation(ClauseEnum clause, String subQuery) {
        this.clause = clause;
        this.subQuery = subQuery;
        this.completed = false;
    }

    public ClauseEnum getClause() {
        return clause;
    }

    public String getSubQuery() {
        return subQuery;
    }

    public boolean isCompleted() {
        return completed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ClauseInvocation that = (ClauseInvocation) o;

        if (completed != that.completed) return false;
        return clause == that.clause && subQuery.equals(that.subQuery);
    }

    @Override
    public int hashCode() {
        int result = clause.hashCode();
        result = 31 * result + subQuery.hashCode();
        result = 31 * result + (completed ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClauseInvocation{" +
                "clause=" + clause +
                ", subQuery='" + subQuery + '\'' +
                ", completed=" + completed +
                '}';
    }
}
