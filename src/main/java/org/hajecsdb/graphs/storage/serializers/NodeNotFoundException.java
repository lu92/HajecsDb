package org.hajecsdb.graphs.storage.serializers;

public class NodeNotFoundException extends Exception {
    public NodeNotFoundException(long nodeId) {
        super("Not found node with nodeId: " + nodeId);
    }
}
