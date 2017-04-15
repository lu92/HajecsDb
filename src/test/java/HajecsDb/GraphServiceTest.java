package HajecsDb;

import org.hajecsdb.graphs.GraphService;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.storage.BinaryGraphStorage;
import org.hajecsdb.graphs.transactions.OperationElement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;
import static org.hajecsdb.graphs.transactions.OperationType.CREATE;

@RunWith(MockitoJUnitRunner.class)
public class GraphServiceTest {

    @Test
    public void createTwoNodesWithRelationship()  throws IOException{
        // given
        BinaryGraphStorage mockBinaryGraphStorage = Mockito.mock(BinaryGraphStorage.class);
        Mockito.doReturn(new GraphImpl("/home", "test")).when(mockBinaryGraphStorage).loadGraph(Mockito.anyString());
        GraphService graphService = new GraphService("/home", "test", mockBinaryGraphStorage);

        graphService.beginTransaction();
        Node firstNode = graphService.createNode();
        Node secondNode = graphService.createNode();
        Relationship relationship = graphService.createRelationship(firstNode, "KNOW", secondNode);

        // when
        graphService.getTransaction().commit();

        // then
        assertThat(graphService.getTransaction().getScope().getOperationsQueue())
                .containsOnly(new OperationElement(firstNode, CREATE), new OperationElement(secondNode, CREATE),
                        new OperationElement(relationship, CREATE));
    }

    @Test
    public void commitWithoutBeginTransactionExpectedTransactionException() {
//        try {
//            GraphService graphService = new GraphService("/home", "test", new BinaryGraphStorage(null));
//            graphService.getTransaction().commit();
//        } catch (IOException e) {
//        } catch (TransactionException e) {
//            assertThat(e.getMessage()).isEqualTo("The transaction has not been started");
//        }
    }

}
