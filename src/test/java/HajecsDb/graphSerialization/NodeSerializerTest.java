package HajecsDb.graphSerialization;

import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.impl.NodeImpl;
import org.hajecsdb.graphs.storage.serializers.NodeNotFoundException;
import org.hajecsdb.graphs.storage.serializers.NodeSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.core.PropertyType.*;

@RunWith(MockitoJUnitRunner.class)
public class NodeSerializerTest {

    private String nodesFilename = "nodes.bin";
    private String nodeMetadataFilename = "nodesMetaData.bin";
    private NodeSerializer nodeSerializer = new NodeSerializer(nodesFilename, nodeMetadataFilename);

    @Before
    public void before() throws IOException {
        clearContentFile(nodesFilename);
        clearContentFile(nodeMetadataFilename);
    }

    @Test
    public void saveAndReadNodeTest() throws IOException {
        // given
        Node node = new NodeImpl(1l);
        Properties properties = new Properties()
                .add("firstName", "JAMES", STRING)
                .add("lastName", "BOND", STRING)
                .add("age", 40, INT);
        node.setProperties(properties);

        // when
        nodeSerializer.save(node);
        Optional<Node> nodeOptional = nodeSerializer.read(1l);

        // then
        assertThat(nodeOptional.isPresent()).isTrue();
        assertThat(nodeOptional.get().getAllProperties().getAllProperties()).hasSize(4);
        assertThat(nodeOptional.get().getAllProperties()).isEqualTo(properties.add("id", 1l, LONG));
    }

    @Test
    public void saveThreeNodesAndReadLastNodeTest() throws IOException {
        // given
        Node node1 = new NodeImpl(1l);
        Properties expectedProperties1 = new Properties()
                .add("firstName", "James", STRING)
                .add("lastName", "BOND", STRING);
        node1.setProperties(expectedProperties1);

        Node node2 = new NodeImpl(2l);
        Properties expectedProperties2 = new Properties()
                .add("firstName", "Hugh", STRING)
                .add("lastName", "Jackman", STRING)
                .add("age", 48, INT);
        node2.setProperties(expectedProperties2);

        Node node3 = new NodeImpl(3l);
        Properties expectedProperties3 = new Properties()
                .add("firstName", "Kate", STRING)
                .add("lastName", "Beckinsale", STRING)
                .add("age", 43, INT)
                .add("height", 170, INT);
        node3.setProperties(expectedProperties3);

        // when
        nodeSerializer.save(node1);
        nodeSerializer.save(node2);
        nodeSerializer.save(node3);
        Optional<Node> nodeOptional = nodeSerializer.read(3);
        assertThat(nodeOptional.isPresent()).isTrue();
        assertThat(nodeOptional.get().getAllProperties().getAllProperties()).hasSize(5);
        assertThat(nodeOptional.get().getAllProperties()).isEqualTo(expectedProperties3.add("id", 3l, LONG));
    }

    @Test
    public void readWhenDbIsEmptyTest() throws IOException {
        // when
        Optional<Node> nodeOptional = nodeSerializer.read(-1);

        // then
        assertThat(nodeOptional.isPresent()).isFalse();
    }

    @Test
    public void readNotExistedNodeWhenDbHasOneNodeTest() throws IOException {
        // given
        Node node = new NodeImpl(1l);
        Properties expectedProperties = new Properties()
                .add("firstName", "James", STRING)
                .add("lastName", "BOND", STRING)
                .add("age", 40, INT);
        node.setProperties(expectedProperties);

        // when
        nodeSerializer.save(node);
        Optional<Node> nodeOptional = nodeSerializer.read(-1);

        // then
        assertThat(nodeOptional.isPresent()).isFalse();
    }

    @Test
    public void readAllWhenDbIsEmptyTest() throws IOException {
        // when
        List<Node> nodes = nodeSerializer.readAll();

        // then
        assertThat(nodes).isEmpty();
    }

    @Test
    public void saveSingleNodeAndReadAllTest() throws IOException {
        // given
        Node node = new NodeImpl(1l);
        Properties expectedProperties = new Properties()
                .add("firstName", "James", STRING)
                .add("lastName", "BOND", STRING)
                .add("age", 40, INT);
        node.setProperties(expectedProperties);

        // when
        nodeSerializer.save(node);
        List<Node> nodes = nodeSerializer.readAll();

        // then
        assertThat(nodes).hasSize(1);
        assertThat(nodes.get(0).getAllProperties().getAllProperties()).hasSize(4);
        assertThat(nodes.get(0).getAllProperties()).isEqualTo(expectedProperties.add("id", 1l, LONG));
    }

    @Test
    public void saveThreeNodesAndReadAllTest() throws IOException {
        // given
        Node node1 = new NodeImpl(1l);
        Properties expectedProperties1 = new Properties()
                .add("firstName", "James", STRING)
                .add("lastName", "BOND", STRING);
        node1.setProperties(expectedProperties1);

        Node node2 = new NodeImpl(2l);
        Properties expectedProperties2 = new Properties()
                .add("firstName", "Hugh", STRING)
                .add("lastName", "Jackman", STRING)
                .add("age", 48, INT);
        node2.setProperties(expectedProperties2);

        Node node3 = new NodeImpl(3l);
        Properties expectedProperties3 = new Properties()
                .add("firstName", "Kate", STRING)
                .add("lastName", "Beckinsale", STRING)
                .add("age", 43, INT)
                .add("height", 170, INT);
        node3.setProperties(expectedProperties3);

        // when
        nodeSerializer.save(node1);
        nodeSerializer.save(node2);
        nodeSerializer.save(node3);
        List<Node> nodes = nodeSerializer.readAll();

        // then
        assertThat(nodes).hasSize(3);
        assertThat(nodes.get(0).getAllProperties().getAllProperties()).hasSize(3);
        assertThat(nodes.get(0).getAllProperties()).isEqualTo(expectedProperties1.add("id", 1l, LONG));
        assertThat(nodes.get(1).getAllProperties().getAllProperties()).hasSize(4);
        assertThat(nodes.get(1).getAllProperties()).isEqualTo(expectedProperties2.add("id", 2l, LONG));
        assertThat(nodes.get(2).getAllProperties().getAllProperties()).hasSize(5);
        assertThat(nodes.get(2).getAllProperties()).isEqualTo(expectedProperties3.add("id", 3l, LONG));
    }


    private void clearContentFile(String filename) throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(filename);
        pw.close();
    }
}
