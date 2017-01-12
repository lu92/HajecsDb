package HajecsDb;

import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.impl.NodeImpl;
import org.hajecsdb.graphs.storage.BinaryGraphStorage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@RunWith(MockitoJUnitRunner.class)
public class BinaryGraphStorageTest {

    @Test
    public void saveNode() throws IOException {
        Path nodePath = Paths.get("/Users/Lucjan","graph.data");

        BinaryGraphStorage graphStorage = new BinaryGraphStorage(nodePath);
        Node node = new NodeImpl(1);
        node.setLabel(new Label("Person"));
//        graphStorage.createNode(node);
        Entity readedNode = graphStorage.readNode(1);
        System.out.println(readedNode);
    }

}
