package HajecsDb.unitTests.transactions;


import org.fest.assertions.Assertions;
import org.hajecsdb.graphs.core.Graph;
import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.impl.GraphImpl;
import org.hajecsdb.graphs.transactions.lockMechanism.LockManager;
import org.hajecsdb.graphs.transactions.lockMechanism.LockNotFoundException;
import org.hajecsdb.graphs.transactions.lockMechanism.LockUnit;
import org.hajecsdb.graphs.transactions.lockMechanism.ReadWriteLock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hajecsdb.graphs.core.ResourceType.NODE;
import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.READ;
import static org.hajecsdb.graphs.transactions.lockMechanism.LockType.WRITE;

@RunWith(MockitoJUnitRunner.class)
public class LockManagerTest {

    @Test
    public void readLockTest() throws LockNotFoundException{
        // given
        Graph graph = new GraphImpl("test", "test");
        Node node1 = graph.createNode();
        Node node2 = graph.createNode();
        Node node3 = graph.createNode();

        LockManager lockManager = new LockManager();

        // when
        lockManager.acquireReadLock(1, node1);
        lockManager.acquireReadLock(1, node2);
        lockManager.acquireReadLock(1, node3);

        // then
        Assertions.assertThat(lockManager.getLock(new LockUnit(NODE, 1))).isEqualTo(new ReadWriteLock(1, READ));
        Assertions.assertThat(lockManager.getLock(new LockUnit(NODE, 2))).isEqualTo(new ReadWriteLock(1, READ));
        Assertions.assertThat(lockManager.getLock(new LockUnit(NODE, 3))).isEqualTo(new ReadWriteLock(1, READ));
    }

    @Test
    public void writeLockTest() throws LockNotFoundException{
        // given
        Graph graph = new GraphImpl("test", "test");
        Node node1 = graph.createNode();
        Node node2 = graph.createNode();
        Node node3 = graph.createNode();

        int transactionId = 1;
        LockManager lockManager = new LockManager();

        // when
        lockManager.acquireWriteLock(transactionId, node1);
        lockManager.acquireWriteLock(transactionId, node2);
        lockManager.acquireWriteLock(transactionId, node3);

        // then
        Assertions.assertThat(lockManager.getLock(new LockUnit(NODE, 1))).isEqualTo(new ReadWriteLock(transactionId, WRITE));
        Assertions.assertThat(lockManager.getLock(new LockUnit(NODE, 2))).isEqualTo(new ReadWriteLock(transactionId, WRITE));
        Assertions.assertThat(lockManager.getLock(new LockUnit(NODE, 3))).isEqualTo(new ReadWriteLock(transactionId, WRITE));
    }

    @Test
    public void releaseLockWhenEntityWasNotLockedExpectedExceptionTest() {
        // given
        Graph graph = new GraphImpl("test", "test");
        Node node = graph.createNode();
        LockManager lockManager = new LockManager();
        long transactionId = 1000;

        // when
        try {
            lockManager.releaseLock(transactionId, node);
        } catch (IllegalArgumentException e) {
            // then
            Assertions.assertThat(e.getMessage()).isEqualTo("NODE [1] is not locked!");
        }
    }

    @Test
    public void ackquireWriteLockAndReleaseTest() {
        // given
        Graph graph = new GraphImpl("test", "test");
        Node node = graph.createNode();
        LockManager lockManager = new LockManager();
        long transactionId = 1000;

        // when
        lockManager.acquireWriteLock(transactionId, node);
        lockManager.releaseLock(transactionId, node);

    }

    @Test
    public void tryToAckquireDoubleWriteLockTestShouldBlockThread() {
        // given
        Graph graph = new GraphImpl("test", "test");
        Node node = graph.createNode();
        LockManager lockManager = new LockManager();
        long transactionId = 1000;

        // when
        lockManager.acquireWriteLock(transactionId, node);
        lockManager.acquireWriteLock(transactionId, node);
    }
}
