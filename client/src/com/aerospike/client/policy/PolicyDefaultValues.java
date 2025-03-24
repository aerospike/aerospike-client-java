/*
 * Copyright (c) 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aerospike.client.policy;

/**
 Container for policy / config defaults
 Note = For Java primitives, only defaults that differ from Java standard primitive defaults or need to be overridden
 due to inheritance  are provided
 (Java primitive defaults are: int=0, boolean=false, String / object = null)
*/
public class PolicyDefaultValues {
    // Static client defaults
    public static final int MAX_CONNECTIONS_PER_NODE = 100;
    public static final int ASYNC_MAX_CONNECTIONS_PER_NODE = -1;

    // Dynamic client defaults
    public static final int TIMEOUT = 1000;
    public static final int ERROR_RATE_WINDOW = 1;
    public static final int MAX_ERROR_RATE = 100;
    public static final boolean FAIL_IF_NOT_CONNECTED = true;
    public static final int LOGIN_TIMEOUT = 5000;
    public static final int MAX_SOCKET_IDLE = 0;
    public static final int TEND_INTERVAL = 1000;

    //Dynamic read defaults
    public static final ReadModeAP READ_MODE_AP = ReadModeAP.ONE;
    public static final ReadModeSC READ_MODE_SC = ReadModeSC.SESSION;
    public static final Replica REPLICA = Replica.SEQUENCE;
    public static final int SOCKET_TIMEOUT = 30000;
    public static final int TOTAL_TIMEOUT = 1000;
    public static final int MAX_RETRIES = 2;

    //Dynamic write defaults
    public static final int MAX_RETRIES_WRITE = 0;

    //Dynamic query defaults
    public static final int RECORD_QUEUE_SIZE = 5000;
    public static final int INFO_TIMEOUT = 1000;
    public static final boolean INCLUDE_BIN_DATA = true;
    public static final int TOTAL_TIMEOUT_QUERY = 0;
    public static final int MAX_RETRIES_QUERY = 5;

    //Dynamic scan defaults
    public static final boolean CONCURRENT_NODES = true;
    public static final int TOTAL_TIMEOUT_SCAN = 0;
    public static final int MAX_RETRIES_SCAN = 5;

    //Dynamic batch defaults
    public static final int MAX_CONCURRENT_THREADS = 1;
    public static final boolean ALLOW_INLINE = true;
    public static final boolean ALLOW_INLINE_SSD = false;
    public static final boolean RESPOND_ALL_KEYS = true;

    //Dynamic batch read defaults
    public static final ReadModeAP READ_MODE_AP_BATCH_READ = ReadModeAP.ONE;
    public static final ReadModeSC READ_MODE_SC_BATCH_READ = ReadModeSC.SESSION;

    //Dynamic TXN roll defaults
    public static final Replica REPLICA_TXN = Replica.MASTER;
    public static final int MAX_RETRIES_TXN = 5;
    public static final int SOCKET_TIMEOUT_TXN = 3000;
    public static final int TOTAL_TIMEOUT_TXN = 10000;
    public static final int SLEEP_BETWEEN_RETRIES = 1000;

    //Dynamic TXN verify defaults
    public static final ReadModeSC READ_MODE_SC_TXN = ReadModeSC.LINEARIZE;

}
