/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.management.config;

public enum TableCommand {
    ADD("add"),
    DELETE("delete");

    public static final String ADD_VALUE = "add";
    public static final String DELETE_VALUE = "delete";

    private final String value;

    /**
     * @param value
     */
    TableCommand(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
