package org.hajecsdb.graphs.storage.serializers;

import com.sun.tools.javac.comp.Infer;
import org.hajecsdb.graphs.core.Entity;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface Serializer<Type extends Entity, BinaryType> {
    BinaryType save(Type type) throws IOException;
    Optional<Type> read(long id) throws IOException;
    List<Type> readAll() throws IOException;
    Type update(Type type) throws IOException;
    void delete(long id) throws IOException, NodeNotFoundException;
}
