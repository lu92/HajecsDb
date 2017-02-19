package org.hajecsdb.graphs.storage.entities;

import java.util.ArrayList;
import java.util.List;

public class RelationshipMetaDataList {
    private long numberOfRelationships;
    private List<RelationshipMetaData> metaDataList;

    public RelationshipMetaDataList(long numberOfRelationships) {
        this.numberOfRelationships = numberOfRelationships;
        this.metaDataList = new ArrayList<>((int)numberOfRelationships);
    }

    public List<RelationshipMetaData> getMetaDataList() {
        return metaDataList;
    }

    public void add(RelationshipMetaData metaData) {
        this.getMetaDataList().add(metaData);
    }

    @Override
    public String toString() {
        return "NodeMetaDataList{" +
                "numberOfRelationships=" + numberOfRelationships +
                ", metaDataList=" + metaDataList +
                '}';
    }
}
