package org.hajecsdb.graphs.storage;

public class BinaryGraph {

    private Header header;

    class Header {
        double version;
        String graphName;
        String pathDir;
        String filename;
        long numberOfNodes;
        long numberOfRelationships;
    }
}
