package org.hajecsdb.graphs.cypher;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;

public class ResultRow {
    private ContentType contentType;
    private Node node;
    private Relationship relationship;
    private String message;
    private Integer intValue;
    private Long longValue;
    private Float floatValue;
    private Double doubleValue;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ResultRow resultRow = (ResultRow) o;

        if (contentType != resultRow.contentType) return false;
        if (node != null ? !node.equals(resultRow.node) : resultRow.node != null) return false;
        if (relationship != null ? !relationship.equals(resultRow.relationship) : resultRow.relationship != null)
            return false;
        return message != null ? message.equals(resultRow.message) : resultRow.message == null;
    }

    @Override
    public int hashCode() {
        int result = contentType != null ? contentType.hashCode() : 0;
        result = 31 * result + (node != null ? node.hashCode() : 0);
        result = 31 * result + (relationship != null ? relationship.hashCode() : 0);
        result = 31 * result + (message != null ? message.hashCode() : 0);
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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getLongValue() {
        return longValue;
    }

    public void setLongValue(Long longValue) {
        this.longValue = longValue;
    }

    public Integer getIntValue() {
        return intValue;
    }

    public void setIntValue(Integer intValue) {
        this.intValue = intValue;
    }

    public Double getDoubleValue() {
        return doubleValue;
    }

    public void setDoubleValue(Double doubleValue) {
        this.doubleValue = doubleValue;
    }

    public Float getFloatValue() {
        return floatValue;
    }

    public void setFloatValue(Float floatValue) {
        this.floatValue = floatValue;
    }
}
