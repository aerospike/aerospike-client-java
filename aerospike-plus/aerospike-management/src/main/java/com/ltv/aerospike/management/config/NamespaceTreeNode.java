/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.management.config;

public enum NamespaceTreeNode {
    ROOT(1),
    NAMESPACE(2),
    SET(3);

    public static final int ROOT_VALUE = 1;
    public static final int NAMESPACE_VALUE = 2;
    public static final int SET_VALUE = 3;

    private final Integer value;

    /**
     * @param value
     */
    NamespaceTreeNode(final Integer value) {
        this.value = value;
    }

    public final Integer getValue() {
        return value;
    }
}
