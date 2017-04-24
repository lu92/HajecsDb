package HajecsDb.integrationTests;

import org.apache.commons.lang3.StringUtils;
import org.hajecsdb.graphs.restLayer.dto.*;

public class Matchers {
    public static boolean sameAs(final ResultRowDto resultRow, final ResultRowDto expectedResultRow) {
        if (resultRow.getContentType() != expectedResultRow.getContentType())
            return false;

        switch (resultRow.getContentType()) {
            case NODE:
                return !sameAs(resultRow.getNode(), expectedResultRow.getNode());

            case RELATIONSHIP:
                return !sameAs(resultRow.getRelationship(), expectedResultRow.getRelationship());

            case STRING:
                return resultRow.getMessage().equals(expectedResultRow.getMessage());

            default:
                throw new IllegalArgumentException("Content type not recognized!");
        }
    }

    public static boolean sameAs(final NodeDto node, final NodeDto expectedNode) {

        if (node.getId() != expectedNode.getId())
            return false;

        if (!StringUtils.equals(node.getLabel(), expectedNode.getLabel()))
            return false;

        if (node.getDegree() != expectedNode.getDegree())
            return false;

        if (node.getRelationships().size() != expectedNode.getRelationships().size() || !node.getRelationships().containsAll(expectedNode.getRelationships()))
            return false;

        PropertiesDto properties = node.getProperties();
        PropertiesDto expectedProperties = expectedNode.getProperties();

        return !(properties.getProperties().size() != expectedProperties.getProperties().size()
                || (properties.getProperties().containsAll(expectedProperties.getProperties())
                && expectedProperties.getProperties().containsAll(properties.getProperties())));
    }

    public static boolean sameAs(final RelationshipDto relationship, final RelationshipDto expectedRelationship) {

        if (relationship.getId() != expectedRelationship.getId())
            return false;

        if (!StringUtils.equals(relationship.getLabel(), expectedRelationship.getLabel()))
            return false;

        if (relationship.getStartNodeId() != expectedRelationship.getStartNodeId())
            return false;

        if (relationship.getEndNodeId() != expectedRelationship.getEndNodeId())
            return false;

        if (relationship.getDirection() != expectedRelationship.getDirection())
            return false;

        PropertiesDto properties = relationship.getProperties();
        PropertiesDto expectedProperties = expectedRelationship.getProperties();

        return !(properties.getProperties().size() != expectedProperties.getProperties().size()
                || (properties.getProperties().containsAll(expectedProperties.getProperties())
                && expectedProperties.getProperties().containsAll(properties.getProperties())));
    }

    public static boolean sameAs(final String message, final String expectedMessage) {
        return StringUtils.equals(message, expectedMessage);
    }

    public static boolean sameAs(final Integer value, final Integer expectedValue) {
        return value.equals(expectedValue);
    }

    public static boolean sameAs(final Long value, final Long expectedValue) {
        return value.equals(expectedValue);
    }

    public static boolean sameAs(final Float value, final Float expectedValue) {
        return value.equals(expectedValue);
    }

    public static boolean sameAs(final Double value, final Double expectedValue) {
        return value.equals(expectedValue);
    }
}
