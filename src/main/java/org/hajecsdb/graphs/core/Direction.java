package org.hajecsdb.graphs.core;


public enum Direction {
    /**
     * Defines outgoing relationships.
     */
    OUTGOING,
    /**
     * Defines incoming relationships.
     */
    INCOMING,
    /**
     * Defines both incoming and outgoing relationships.
     */
    BOTH;

    public Direction reverse()
    {
        switch ( this )
        {
            case OUTGOING:
                return INCOMING;
            case INCOMING:
                return OUTGOING;
            case BOTH:
                return BOTH;
            default:
                throw new IllegalStateException( "Unknown Direction "
                        + "enum: " + this );
        }
    }
}