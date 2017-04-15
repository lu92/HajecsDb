package org.hajecsdb.graphs.cypher;


import org.hajecsdb.graphs.cypher.clauses.DFA.DFA;
import org.hajecsdb.graphs.cypher.clauses.DFA.State;
import org.hajecsdb.graphs.cypher.clauses.*;

final class CypherDfaBuilder {
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

    public CypherDfaBuilder() {
        dfa = new DFA();
        createNodeClauseBuilder = new CreateNodeClauseBuilder();
        createRelationshipClauseBuilder = new CreateRelationshipClauseBuilder();
        matchNodeClauseBuilder = new MatchNodeClauseBuilder();
        matchRelationshipClauseBuilder = new MatchRelationshipClauseBuilder();
        whereClauseBuilder = new WhereClauseBuilder();
        deleteNodeClaudeBuilder = new DeleteNodeClaudeBuilder();
        setClauseBuilder = new SetClauseBuilder();
        removeClauseBuilder = new RemoveClauseBuilder();
        returnClauseBuilder = new ReturnClauseBuilder();
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
