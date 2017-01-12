package org.hajecsdb.graphs.core;


public interface Entity extends PropertyContainer{
    String LABEL = "label";
    String ID = "id";

    long getId();
}
