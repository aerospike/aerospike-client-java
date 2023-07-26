/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.proxy;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;

/**
 * Factory class AerospikeClientFactory will generate either a native client or a proxy client,
 * based on whether isProxy is true or false.  You can use this factory class in conjunction with your own
 * logic to select native or proxy to create an application that can automatically work with both the
 * native and proxy-based versions of Aerospike.
 */
public class AerospikeClientFactory {
    /**
     * Return either a native Aerospike client or a proxy client, based on isProxy.
     *
     * @param clientPolicy              clientPolicy to pass to Client constructor
     * @param hosts                     Host array to pass to Client constructor
     * @param isProxy                   If true, return AerospikeClientProxy - otherwise return AerospikeClient
     * @return IAerospikeClient
     */
    public static IAerospikeClient getClient(ClientPolicy clientPolicy, Host[] hosts, boolean isProxy) {
        if (isProxy) {
            return new AerospikeClientProxy(clientPolicy, hosts);
        }
        else {
            return new AerospikeClient(clientPolicy, hosts);
        }
    }
}