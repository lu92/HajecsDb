package org.hajecsdb.graphs.storage.serializers;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.NotFoundException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface Serializer<Type extends Entity, BinaryType> {
    BinaryType save(Type type) throws IOException;
    Optional<Type> read(long id) throws IOException;
    List<Type> readAll() throws IOException;
    BinaryType update(Type type) throws IOException, NotFoundException;
    void delete(long id) throws IOException, NotFoundException;
    long count() throws IOException;
}
