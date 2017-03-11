package org.hajecsdb.graphs.cypher;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;

public enum CommandType {
    CREATE_NODE(1),
    MATCH_NODE(1),
    WHERE(2),
    REMOVE(3),
    RETURN(4);


    CommandType(int priority) {
        this.priority = priority;
    }

    private int priority;
}
