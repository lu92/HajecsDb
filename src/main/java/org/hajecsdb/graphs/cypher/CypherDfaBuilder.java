package org.hajecsdb.graphs.cypher;


import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.clauses.*;

public class CypherDfaBuilder {
    private DFA dfa;
    private CreateNodeClauseBuilder createNodeClauseBuilder;
    private MatchNodeClauseBuilder matchNodeClauseBuilder;
    private WhereClauseBuilder whereClauseBuilder;
    private DeleteNodeClaudeBuilder deleteNodeClaudeBuilder;
    private SetClauseBuilder setClauseBuilder;
    private RemoveNodeLabelClauseBuilder removeNodeLabelClauseBuilder;

    public CypherDfaBuilder(Graph graph) {
        dfa = new DFA();
        createNodeClauseBuilder = new CreateNodeClauseBuilder(graph);
        matchNodeClauseBuilder = new MatchNodeClauseBuilder(graph);
        whereClauseBuilder = new WhereClauseBuilder(graph);
        deleteNodeClaudeBuilder = new DeleteNodeClaudeBuilder(graph);
        setClauseBuilder = new SetClauseBuilder(graph);
        removeNodeLabelClauseBuilder = new RemoveNodeLabelClauseBuilder(graph);
    }

    public void buildClauses() {
        State beginState = dfa.getBeginState();
        createNodeClauseBuilder.buildClause(dfa, beginState);
        State matchClauseEndState = matchNodeClauseBuilder.buildClause(dfa, beginState);
        State whereClauseEndState = whereClauseBuilder.buildClause(dfa, matchClauseEndState);
        setClauseBuilder.buildClause(dfa, matchClauseEndState);
        setClauseBuilder.buildClause(dfa, whereClauseEndState);
        deleteNodeClaudeBuilder.buildClause(dfa, matchClauseEndState);
        deleteNodeClaudeBuilder.buildClause(dfa, whereClauseEndState);
        removeNodeLabelClauseBuilder.buildClause(dfa, matchClauseEndState);
        removeNodeLabelClauseBuilder.buildClause(dfa, whereClauseEndState);
    }

    public DFA getDfa() {
        return dfa;
    }
}
