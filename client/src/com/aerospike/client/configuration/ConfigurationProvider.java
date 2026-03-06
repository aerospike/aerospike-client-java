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

package com.aerospike.client.configuration;

import com.aerospike.client.configuration.serializers.Configuration;

/**
 * Supplies static and dynamic client configuration (e.g. from YAML). Pass to {@link com.aerospike.client.policy.ClientPolicy#ClientPolicy(com.aerospike.client.policy.ClientPolicy, com.aerospike.client.configuration.ConfigurationProvider)} so policies can load and apply config.
 * <p>
 * The client and policy constructors that take a ConfigurationProvider use it to populate policy fields from the fetched {@link Configuration}. Implement this interface to provide config from a custom source.
 * <pre>{@code
 * ConfigurationProvider provider = YamlConfigProvider.getConfigProvider("file:///path/to/config.yaml");
 * ClientPolicy clientPolicy = new ClientPolicy(new ClientPolicy(), provider);
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 *
 * ConfigurationProvider fromClient = client.getConfigProvider();
 * if (fromClient != null && fromClient.loadConfiguration()) {
 *   Configuration config = fromClient.fetchConfiguration();
 * }
 * }</pre>
 *
 * @see Configuration
 * @see YamlConfigProvider
 * @see com.aerospike.client.policy.ClientPolicy#ClientPolicy(ClientPolicy, ConfigurationProvider)
 * @see com.aerospike.client.IAerospikeClient#getConfigProvider()
 */
public interface ConfigurationProvider {
    /**
     * Loads or reloads configuration from the provider source.
     * @return true if configuration was loaded successfully
     */
    boolean loadConfiguration();

    /**
     * Returns the current static and dynamic configuration snapshot.
     * @return configuration (must not be null)
     */
    Configuration fetchConfiguration();

    /**
     * Returns the dynamic part of configuration (for runtime updates).
     * @return dynamic configuration (must not be null)
     */
    Configuration fetchDynamicConfiguration();
}
