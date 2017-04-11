package org.hajecsdb.graphs.cypher;


import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.cypher.DFA.*;
import org.hajecsdb.graphs.cypher.clauses.*;

public class CypherDfaBuilder {
    private DFA dfa;
    private CreateNodeClauseBuilder createNodeClauseBuilder;
    private CreateRelationshipClauseBuilder createRelationshipClauseBuilder;
    private MatchNodeClauseBuilder matchNodeClauseBuilder;
    private MatchRelationshipClauseBuilder matchRelationshipClauseBuilder;
    private WhereClauseBuilder whereClauseBuilder;
    private DeleteNodeClaudeBuilder deleteNodeClaudeBuilder;
    private SetClauseBuilder setClauseBuilder;
    private RemoveClauseBuilder removeClauseBuilder;
    private ReturnClauseBuilder returnClauseBuilder;

    public CypherDfaBuilder(Graph graph) {
        dfa = new DFA();
        createNodeClauseBuilder = new CreateNodeClauseBuilder(graph);
        createRelationshipClauseBuilder = new CreateRelationshipClauseBuilder(graph);
        matchNodeClauseBuilder = new MatchNodeClauseBuilder(graph);
        matchRelationshipClauseBuilder = new MatchRelationshipClauseBuilder(graph);
        whereClauseBuilder = new WhereClauseBuilder(graph);
        deleteNodeClaudeBuilder = new DeleteNodeClaudeBuilder(graph);
        setClauseBuilder = new SetClauseBuilder(graph);
        removeClauseBuilder = new RemoveClauseBuilder(graph);
        returnClauseBuilder = new ReturnClauseBuilder(graph);
    }

    public void buildClauses() {
        State beginState = dfa.getBeginState();
        State createNodeClauseEndState = createNodeClauseBuilder.buildClause(dfa, beginState);
        State matchNodeClauseEndState = matchNodeClauseBuilder.buildClause(dfa, beginState);
        State createRelationshipClauseEndState = createRelationshipClauseBuilder.buildClause(dfa, matchNodeClauseEndState);
        State matchRelationshipClauseEndState = matchRelationshipClauseBuilder.buildClause(dfa, beginState);
        State whereClauseEndState = whereClauseBuilder.buildClause(dfa, matchNodeClauseEndState);
        setClauseBuilder.buildClause(dfa, matchNodeClauseEndState);
        setClauseBuilder.buildClause(dfa, whereClauseEndState);
        deleteNodeClaudeBuilder.buildClause(dfa, matchNodeClauseEndState);
        deleteNodeClaudeBuilder.buildClause(dfa, whereClauseEndState);
        removeClauseBuilder.buildClause(dfa, matchNodeClauseEndState);
        removeClauseBuilder.buildClause(dfa, whereClauseEndState);
        returnClauseBuilder.buildClause(dfa, createNodeClauseEndState);
        returnClauseBuilder.buildClause(dfa, matchNodeClauseEndState);
        returnClauseBuilder.buildClause(dfa, createRelationshipClauseEndState);
        returnClauseBuilder.buildClause(dfa, matchRelationshipClauseEndState);
        returnClauseBuilder.buildClause(dfa, whereClauseEndState);
    }

    public DFA getDfa() {
        return dfa;
    }
}
