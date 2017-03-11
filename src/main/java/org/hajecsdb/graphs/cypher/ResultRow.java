package org.hajecsdb.graphs.cypher;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;

public class ResultRow {
    private ContentType contentType;
    private Node node;
    private Relationship relationship;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ResultRow resultRow = (ResultRow) o;

        if (contentType != resultRow.contentType) return false;
        if (node != null ? !node.equals(resultRow.node) : resultRow.node != null) return false;
        return relationship != null ? relationship.equals(resultRow.relationship) : resultRow.relationship == null;
    }

    @Override
    public int hashCode() {
        int result = contentType.hashCode();
        result = 31 * result + (node != null ? node.hashCode() : 0);
        result = 31 * result + (relationship != null ? relationship.hashCode() : 0);
        return result;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public void setContentType(ContentType contentType) {
        this.contentType = contentType;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Relationship getRelationship() {
        return relationship;
    }

    public void setRelationship(Relationship relationship) {
        this.relationship = relationship;
    }

}
