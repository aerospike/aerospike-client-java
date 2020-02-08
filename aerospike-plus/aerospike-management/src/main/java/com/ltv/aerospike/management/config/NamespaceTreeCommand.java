/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.management.config;

public enum NamespaceTreeCommand {
    CREATE_NAMESPACE("create_namespace"),
    EDIT_NAMESPACE("edit_namespace"),
    DELETE_NAMESPACE("delete_namespace"),
    CREATE_SET("create_set"),
    EDIT_SET("edit_set"),
    DELETE_SET("delete_set");

    public static final String CREATE_NAMESPACE_VALUE = "create_namespace";
    public static final String EDIT_NAMESPACE_VALUE = "edit_namespace";
    public static final String DELETE_NAMESPACE_VALUE = "delete_namespace";
    public static final String CREATE_SET_VALUE = "create_set";
    public static final String EDIT_SET_VALUE = "edit_set";
    public static final String DELETE_SET_VALUE = "delete_set";

    private final String value;

    /**
     * @param value
     */
    NamespaceTreeCommand(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
