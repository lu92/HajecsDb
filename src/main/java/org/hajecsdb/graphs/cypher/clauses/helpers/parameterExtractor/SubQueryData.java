package org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor;

public final class SubQueryData {
    private NodeData leftNode;
    private RelationshipData relationship;
    private NodeData rightNode;

    public SubQueryData(NodeData leftNode, RelationshipData relationship, NodeData rightNode) {
        this.leftNode = leftNode;
        this.relationship = relationship;
        this.rightNode = rightNode;
    }

    public NodeData getLeftNode() {
        return leftNode;
    }

    public RelationshipData getRelationship() {
        return relationship;
    }

    public NodeData getRightNode() {
        return rightNode;
    }
}
