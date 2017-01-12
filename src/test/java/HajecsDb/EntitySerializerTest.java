package HajecsDb;

import org.hajecsdb.graphs.core.Label;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.impl.NodeImpl;
import org.hajecsdb.graphs.storage.serializers.EntitySerializer;
import org.hajecsdb.graphs.storage.EntityType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

@RunWith(MockitoJUnitRunner.class)
public class EntitySerializerTest {

    private EntitySerializer entitySerializer = new EntitySerializer();

    private void clearFile(Path path) {
        try {
            Files.delete(path);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", path);
        } catch (IOException e) {

        }
    }

    @Test
    public void serializeEmptyNodeTest() throws IOException {
        // given
//        clearFile(nodePath);
        Path nodesPath = Paths.get("/Users/Lucjan", "nodes.bin");
        Path nodesMetaDataPath = Paths.get("/Users/Lucjan", "nodes_metadata.bin");

        Node node = new NodeImpl(Long.MAX_VALUE);
        node.setLabel(new Label("Person"));
        node.setProperties(new Properties().add("name", "Lucjan", PropertyType.STRING).add("age", 25, PropertyType.INT));

        // when
        entitySerializer.serializeEntity(nodesPath, nodesMetaDataPath, node, EntityType.NODE);


        //then
//        Assertions.assertThat()
    }
}
