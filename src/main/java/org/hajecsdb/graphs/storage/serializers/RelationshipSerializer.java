package org.hajecsdb.graphs.storage.serializers;

import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.core.impl.RelationshipImpl;

public class RelationshipSerializer extends EntitySerializer<Relationship> {

    public RelationshipSerializer(String relationshipsFilename, String relationshipMetadataFilename) {
        super(relationshipsFilename, relationshipMetadataFilename);
    }

    @Override
    Relationship buildEntityType(long id, Properties properties) {
        RelationshipImpl relationship = new RelationshipImpl(id);
        relationship.setProperties(properties);
        return relationship;
    }

}
