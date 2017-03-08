package org.hajecsdb.graphs.cypher;


import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.clauses.CreateNodeClauseBuilder;
import org.hajecsdb.graphs.cypher.clauses.MatchNodeClauseBuilder;

public class CypherDfaBuilder {
    private DFA dfa = new DFA();
    private CreateNodeClauseBuilder createNodeClauseBuilder = new CreateNodeClauseBuilder();
    private MatchNodeClauseBuilder matchNodeClauseBuilder = new MatchNodeClauseBuilder();

    public void buildClauses() {
        createNodeClauseBuilder.buildClause(dfa);
        matchNodeClauseBuilder.buildClause(dfa);
    }

    public DFA getDfa() {
        return dfa;
    }
}
