package com.aerospike.client.tran;

public enum TranOp {
    // TODO: Make this a flags bitmap.
    NONE(0),
    GET_VERSION_ONLY(1),
    ROLL_FORWARD(2),
    ROLL_BACK(4);

    public final int attr;

    TranOp(int attr) {
        this.attr = attr;
    }
}
