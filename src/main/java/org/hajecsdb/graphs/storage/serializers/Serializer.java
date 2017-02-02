package org.hajecsdb.graphs.storage.serializers;

import org.hajecsdb.graphs.core.Entity;

import java.io.IOException;

public interface Serializer<Type extends Entity, BinaryType> {
    BinaryType save(Type type) throws IOException;
    Type read(long id) throws IOException;
    Type update(Type type) throws IOException;
    void delete(long id) throws IOException;
}
