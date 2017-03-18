package HajecsDb.utils;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;

import java.util.Optional;

public class NodeComparator {

    public boolean isSame(Node node1, Node node2) {
        Properties propertiesOfFirstNode = node1.getAllProperties();
        Properties propertiesOfSecondNode = node2.getAllProperties();

        if (propertiesOfFirstNode.getAllProperties().size()
                == propertiesOfSecondNode.getAllProperties().size()) {
            for (Property searchedProperty : propertiesOfFirstNode.getAllProperties()) {
                Optional<Property> propertyOptional = propertiesOfSecondNode.getProperty(searchedProperty.getKey());
                if (!propertyOptional.isPresent() || !propertyOptional.get().equals(searchedProperty)) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }
}
