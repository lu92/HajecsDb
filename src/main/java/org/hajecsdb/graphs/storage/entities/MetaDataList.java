package org.hajecsdb.graphs.storage.entities;

import java.util.ArrayList;
import java.util.List;

public class MetaDataList {
    private long numberOfEntities;
    private List<MetaData> metaDataList;

    public MetaDataList(long numberOfEntities) {
        this.numberOfEntities = numberOfEntities;
        this.metaDataList = new ArrayList<>((int) this.numberOfEntities);
    }

    public List<MetaData> getMetaDataList() {
        return metaDataList;
    }

    public void add(MetaData metaData) {
        this.getMetaDataList().add(metaData);
    }

    @Override
    public String toString() {
        return "MetaDataList{" +
                "numberOfEntities=" + numberOfEntities +
                ", metaDataList=" + metaDataList +
                '}';
    }
}