package HajecsDb.unitTests.transactions;


import org.fest.assertions.Assertions;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.transactions.lockMechanism.RWLock;
import org.hajecsdb.graphs.transactions.lockMechanism.LockManager;
import org.hajecsdb.graphs.transactions.lockMechanism.LockNotFoundException;
import org.hajecsdb.graphs.transactions.lockMechanism.LockUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hajecsdb.graphs.core.ResourceType.NODE;
import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.READ;

@RunWith(MockitoJUnitRunner.class)
public class LockManagerTest {

    @Test
    public void test() throws LockNotFoundException{
        // given
        Graph graph = new GraphImpl("test", "test");
        Node node1 = graph.createNode();
        Node node2 = graph.createNode();
        Node node3 = graph.createNode();

        LockManager lockManager = new LockManager();

        // when
        lockManager.acquireReadLock(node1);
        lockManager.acquireReadLock(node2);
        lockManager.acquireReadLock(node3);

        // then
        Assertions.assertThat(lockManager.getReadLock(new LockUnit(NODE, 1))).isEqualTo(new RWLock(READ));
        Assertions.assertThat(lockManager.getReadLock(new LockUnit(NODE, 2))).isEqualTo(new RWLock(READ));
        Assertions.assertThat(lockManager.getReadLock(new LockUnit(NODE, 3))).isEqualTo(new RWLock(READ));
    }
}
