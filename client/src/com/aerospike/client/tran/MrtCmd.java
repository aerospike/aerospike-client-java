package com.aerospike.client.tran;

public enum MrtCmd {
    // TODO: Make this a flags bitmap.
    MRT_VERIFY_READ(0),  // TODO: Change to 1 when start using.
    MRT_ROLL_FORWARD(2),
    MRT_ROLL_BACK(4);

    public final int attr;

    MrtCmd(int attr) {
        this.attr = attr;
    }
}
