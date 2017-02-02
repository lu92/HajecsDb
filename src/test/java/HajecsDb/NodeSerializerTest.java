package HajecsDb;

import org.fest.assertions.Assertions;
import org.fest.util.Files;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.impl.NodeImpl;
import org.hajecsdb.graphs.storage.serializers.BinaryNode;
import org.hajecsdb.graphs.storage.serializers.NodeSerializer;
import org.hajecsdb.graphs.storage.serializers.PropertiesBinaryMapper;
import org.hajecsdb.graphs.storage.serializers.Serializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.INT;
import static org.hajecsdb.graphs.core.PropertyType.STRING;

@RunWith(MockitoJUnitRunner.class)
public class NodeSerializerTest {

//    private Path nodesPath = Paths.get(getClass().getResource("nodes.bin").toURI());
//    private Serializer serializer = new NodeSerializer(nodesPath, null);
    Path nodesPath = Paths.get("/Users/Lucjan", "nodes.bin");


    @Test
    public void test() throws IOException, URISyntaxException {
        // given
        Serializer serializer = new NodeSerializer(nodesPath, null);
        Node node = new NodeImpl(1l);
        Properties properties = new Properties()
                .add("firstName", "James", STRING)
                .add("lastName", "Bond", STRING)
                .add("age", 40, INT);
        node.setProperties(properties);

        // when
        BinaryNode binaryNode = (BinaryNode) serializer.save(node);

        // then
        assertThat(binaryNode).isNotNull();
        assertThat(binaryNode.getNodeId()).isEqualTo(1l);
        new PropertiesBinaryMapper().toProperties(binaryNode.getPropertiesInBinaryFigure());
    }

}
