package org.hajecsdb.graphs.cypher.clauses.helpers.parameterExtractor;

import org.hajecsdb.graphs.core.Direction;
import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class ParameterExtractor {

    public List<Property> extractParameters(String parametersBody) {
        List<Property> parameters = new ArrayList<>();
        if (isNotEmpty(parametersBody)) {
            String paramRegex = "([\\w]*):([\\w'.]*)";
            Pattern paramPattern = Pattern.compile(paramRegex);
            Matcher paramsMatcher = paramPattern.matcher(parametersBody);

            while (paramsMatcher.find()) {
                String variable = paramsMatcher.group(1);
                String value = paramsMatcher.group(2);
                Property property = extract(variable, value);
                parameters.add(property);
            }
        }
        return parameters;
    }

    public Property extract(String key, String value) {
        Property property;
        if (isStringType(value)) {
            property = new Property(key, PropertyType.STRING, value.substring(1, value.length() - 1));
        } else if (isIntType(value)) {
            property = new Property(key, PropertyType.INT, new Integer(value));
        } else if (isDoubleType(value)) {
            property = new Property(key, PropertyType.DOUBLE, new Double(value));
        } else
            throw new NotImplementedException();

        return property;
    }

    public SubQueryData fetchData(String text) {
        String regex = "(\\([\\w :'{}]+\\))([-><:_\\[\\]\\w]*)?(\\([\\w :'{}]+\\))";
        Matcher mather = Pattern.compile(regex).matcher(text);

        if (mather.find()) {

            String leftNodePart = mather.group(1);
            String relationshipPart = mather.group(2);
            String rightNodePart = mather.group(3);

            SubQueryData subQueryData = new SubQueryData(
                    fetchNode(leftNodePart),
                    fetchRelationship(relationshipPart),
                    fetchNode(rightNodePart));

            return subQueryData;
        }
        throw new IllegalArgumentException("Cannot read relationship!");
    }

    public NodeData fetchNode(String nodeContent) {
        String nodeRegex = "\\(([\\w]+):?([\\w]+)?(\\{[\\w:' }]+)?\\)";
        Matcher matcher = Pattern.compile(nodeRegex).matcher(nodeContent);


        if (matcher.find()) {
            Optional<String> variableName = Optional.ofNullable(matcher.group(1));
            Optional<Label> label = isNotEmpty(matcher.group(2)) ? Optional.of(new Label(matcher.group(2))) : Optional.empty();
            List<Property> parameters = new LinkedList<>();

            if (isNotEmpty(matcher.group(3))) {
                String paramContent = matcher.group(3);
                String paramRegex = "([\\w]*):([\\w'.]*)";
                Pattern paramPattern = Pattern.compile(paramRegex);
                Matcher paramsMatcher = paramPattern.matcher(paramContent);

                while (paramsMatcher.find()) {
                    String variable = paramsMatcher.group(1);
                    String value = paramsMatcher.group(2);
                    Property property = extract(variable, value);
                    parameters.add(property);
                }
            }
            return new NodeData(variableName, label, parameters);
        }
        throw new IllegalArgumentException("");
    }

    private RelationshipData fetchRelationship(String relationshipContent) {
        return Arrays.asList(notDirectedRelationship(relationshipContent),
                incomingRelationship(relationshipContent),
                outgoingRelationship(relationshipContent)).stream()
                .filter(relationshipData -> relationshipData.isPresent()).findFirst().get()
                .orElseThrow(() -> new IllegalArgumentException("INTERNAL ERROR!"));
    }

    private Optional<RelationshipData> notDirectedRelationship(String relationshipContent) {
        if (relationshipContent.equals("--")) {
            return Optional.of(new RelationshipData(null, null, Direction.BOTH));
        }
        return Optional.empty();
    }

    private Optional<RelationshipData> incomingRelationship(String relationshipContent) {
        String relationshipRegex = "(<-\\[([\\w]+):?([\\w]+)?\\]-)";
        Matcher matcher = Pattern.compile(relationshipRegex).matcher(relationshipContent);
        if (matcher.find()) {
            Optional<String> variable = Optional.ofNullable(matcher.group(2));
            Optional<Label> label = isNotEmpty(matcher.group(3)) ? Optional.of(new Label(matcher.group(3))) : Optional.empty();
            Direction direction = Direction.INCOMING;
            return Optional.of(new RelationshipData(variable, label, direction));
        }
        return Optional.empty();
    }

    private Optional<RelationshipData> outgoingRelationship(String relationshipContent) {
        String relationshipRegex = "(-\\[([\\w]+):?([\\w]+)?\\]->)";
        Matcher matcher = Pattern.compile(relationshipRegex).matcher(relationshipContent);
        if (matcher.find()) {
            Optional<String> variable = Optional.ofNullable(matcher.group(2));
            Optional<Label> label = isNotEmpty(matcher.group(3)) ? Optional.of(new Label(matcher.group(3))) : Optional.empty();
            Direction direction = Direction.OUTGOING;
            return Optional.of(new RelationshipData(variable, label, direction));
        }
        return Optional.empty();
    }


    private boolean isStringType(String value) {
        return value.contains("'");
    }

    private boolean isIntType(String value) {
        return value.matches("[\\d]+");
    }

    private boolean isDoubleType(String value) {
        return value.matches("[\\d]+.[\\d]+");
    }
}