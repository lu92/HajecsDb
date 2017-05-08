package org.hajecsdb.graphs.transactions.lockMechanism;

public class LockNotFoundException extends Exception {
    public LockNotFoundException() {
        super("Lock not found!");
    }
}
