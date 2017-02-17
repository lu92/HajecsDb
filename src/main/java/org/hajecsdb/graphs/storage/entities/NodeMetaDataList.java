package org.hajecsdb.graphs.storage.entities;

import java.util.ArrayList;
import java.util.List;

public class NodeMetaDataList {
    private long numberOfNodes;
    private List<NodeMetaData> nodeMetaDataList;

    public NodeMetaDataList() {
        nodeMetaDataList = new ArrayList<>();
    }

    public NodeMetaDataList(long numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        this.nodeMetaDataList = new ArrayList<>((int)numberOfNodes);
    }

    public long getNumberOfNodes() {
        return numberOfNodes;
    }

    public void setNumberOfNodes(long numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    public List<NodeMetaData> getNodeMetaDataList() {
        return nodeMetaDataList;
    }

    public void setNodeMetaDataList(List<NodeMetaData> nodeMetaDataList) {
        this.nodeMetaDataList = nodeMetaDataList;
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
