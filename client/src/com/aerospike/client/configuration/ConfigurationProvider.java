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
 * Supplies client configuration (static and dynamic). Used by policies and the client to load
 * and fetch configuration.
 * <p>
 * Implementations: {@link YamlConfigProvider} (YAML-based). Application code may implement this
 * interface for custom configuration sources.
 * <p>Use YamlConfigProvider from a file path, or implement a custom provider that returns Configuration from another source.</p>
 * <pre>{@code
 * // Built-in YAML provider (e.g. from env AEROSPIKE_CLIENT_CONFIG_URL or path)
 * ConfigurationProvider provider = YamlConfigProvider.getConfigProvider("/path/to/config.yaml");
 * if (provider != null && provider.loadConfiguration()) {
 *   Configuration config = provider.fetchConfiguration();
 * }
 *
 * // Custom provider (e.g. from remote or in-memory)
 * ConfigurationProvider custom = new ConfigurationProvider() {
 *   public boolean loadConfiguration() { return true; }
 *   public Configuration fetchConfiguration() { return myConfig; }
 *   public Configuration fetchDynamicConfiguration() { return myDynamicConfig; }
 * };
 * }</pre>
 *
 * @see YamlConfigProvider
 */
public interface ConfigurationProvider {
    boolean loadConfiguration();

    Configuration fetchConfiguration();
    Configuration fetchDynamicConfiguration();
}
