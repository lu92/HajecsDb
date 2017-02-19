package org.hajecsdb.graphs.storage.entities;

import java.util.ArrayList;
import java.util.List;

public class NodeMetaDataList {
    private long numberOfNodes;
    private List<NodeMetaData> nodeMetaDataList;

    public NodeMetaDataList(long numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        this.nodeMetaDataList = new ArrayList<>((int)numberOfNodes);
    }

    public List<NodeMetaData> getNodeMetaDataList() {
        return nodeMetaDataList;
    }

    public void add(NodeMetaData metaData) {
        this.getNodeMetaDataList().add(metaData);
    }

    @Override
    public String toString() {
        return "NodeMetaDataList{" +
                "numberOfNodes=" + numberOfNodes +
                ", nodeMetaDataList=" + nodeMetaDataList +
                '}';
    }
}
