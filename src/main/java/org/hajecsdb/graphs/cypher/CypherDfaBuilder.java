package org.hajecsdb.graphs.cypher;


import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.clauses.CreateNodeClauseBuilder;
import org.hajecsdb.graphs.cypher.clauses.MatchNodeClauseBuilder;
import org.hajecsdb.graphs.cypher.clauses.WhereClauseBuilder;

public class CypherDfaBuilder {
    private DFA dfa;
    private CreateNodeClauseBuilder createNodeClauseBuilder;
    private MatchNodeClauseBuilder matchNodeClauseBuilder;
    private WhereClauseBuilder whereClauseBuilder;

    public CypherDfaBuilder(Graph graph) {
        dfa = new DFA();
        createNodeClauseBuilder = new CreateNodeClauseBuilder(graph);
        matchNodeClauseBuilder = new MatchNodeClauseBuilder(graph);
        whereClauseBuilder = new WhereClauseBuilder(graph);
    }

    public void buildClauses() {
        State beginState = dfa.getBeginState();
        State nodeClauseEndState = createNodeClauseBuilder.buildClause(dfa, beginState);
        State matchClauseEndState = matchNodeClauseBuilder.buildClause(dfa, beginState);
        whereClauseBuilder.buildClause(dfa, matchClauseEndState);
    }

    public DFA getDfa() {
        return dfa;
    }
}
