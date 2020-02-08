/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.util;

import io.grpc.Context;

public class AppConstant {
    public static final int PAGE_LENGTH = 10;
    public static final String TABLE_USER_ID = "1";
    public static final String TABLE_NAMESPACE_ID = "2";
    public static final String TABLE_SET_ID = "3";
    public static final String TABLE_SEQUENCE_ID = "4";
    public static final String TABLE_INDEX_ID = "5";
    public static final String TABLE_SESSION_ID = "6";
    public static final String TABLE_META_ID = "7";
    public static final String TABLE_USER = "user";
    public static final String TABLE_NAMESPACE = "namespace";
    public static final String TABLE_SET = "set";
    public static final String TABLE_SEQUENCE = "sequence";
    public static final String TABLE_INDEX = "index";
    public static final String TABLE_SESSION = "session";
    public static final String TABLE_META = "meta";
    //public static final String TABLE_TRANSACTION = "transaction";

    public static final String ROOT_USER_ID = "1";
    public static final String ROOT_USER_NAME = "root";
    public static final String AEROSPIKE_NAMESPACE_ID = "1";
    public static final String AEROSPIKE_NAMESPACE_NAME = "aerospike";
    public static final String ROLE_DBA = "DBA";
    public static final String ROLE_USER = "USER";
    public static final String ROLE_DQL = "dql";
    public static final String ROLE_DML = "dml";
    public static final String ROLE_DDL = "ddl";
    public static final String ROLE_DCL = "dcl";
    public static final String ROLE_OWNER = "owner";
    public static final String PERMISSION_SELECT = "grant_select";
    public static final String PERMISSION_PUT = "grant_put";
    public static final String PERMISSION_DELETE = "grant_delete";
    public static final String DEL_FLAG = "del_flag";
    public static final String CREATE_AT = "create_at";
    public static final String UPDATE_AT = "update_at";
    public static final Context.Key<String> AUTHEN_TOKEN_KEY = Context.key("authen_token");

    public static final String STRING = "string";
    public static final String INTEGER = "integer";
    public static final String LONG = "long";
    public static final String FLOAT = "float";
    public static final String DOUBLE = "double";
    public static final String BOOLEAN = "boolean";
    public static final String DATE = "date";
    public static final String KEY = "key";
}
