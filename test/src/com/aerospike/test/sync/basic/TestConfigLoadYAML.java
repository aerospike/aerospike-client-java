/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.*;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.YamlConfigProvider;
import com.aerospike.test.sync.TestSync;

public class TestConfigLoadYAML extends TestSync {
	public static final String GOOD_YAML_CONF_RELATIVE_PATH = "/src/resources/aerospikeconfig.yaml";
	public static final String BOGUS_YAML_IP_CONF_RELATIVE_PATH = "/src/resources/bogus_invalid_property.yaml";
	public static final String BOGUS_YAML_IF_CONF_RELATIVE_PATH = "/src/resources/bogus_invalid_format.yaml";
	public static final String BOGUS_YAML_MVer_CONF_RELATIVE_PATH = "/src/resources/bogus_missing_version.yaml";
	public static final String BOGUS_YAML_IV_CONF_RELATIVE_PATH = "/src/resources/bogus_invalid_version.yaml";
	public static final String YAML_URL_BASE = "file://" + System.getProperty("user.dir");

	@Test
	public void loadGoodYaml() {
		String yamlURL = YAML_URL_BASE + GOOD_YAML_CONF_RELATIVE_PATH;
		System.setProperty("AEROSPIKE_CLIENT_CONFIG_SYS_PROP", yamlURL);
		YamlConfigProvider yamlLoader = new YamlConfigProvider(yamlURL);
		Configuration yamlConf = yamlLoader.fetchConfiguration();
		assertNotNull(yamlConf);
		// System.out.println(yamlConf);
		assertNotNull(yamlConf.dynamicConfiguration.dynamicClientConfig.appId);
		assert yamlConf.dynamicConfiguration.dynamicClientConfig.appId.value.equals("unit_tester");
		assert yamlConf.staticConfiguration.staticClientConfig.maxConnectionsPerNode.value == 99;
	}
	@Test
	public void loadBogusYamlInvalidProperty() {
		String yamlURL = YAML_URL_BASE + BOGUS_YAML_IP_CONF_RELATIVE_PATH;
		System.setProperty("AEROSPIKE_CLIENT_CONFIG_SYS_PROP", yamlURL);
		YamlConfigProvider yamlLoader = new YamlConfigProvider(yamlURL);
		Configuration yamlConf = yamlLoader.fetchConfiguration();
		assertNotNull(yamlConf);
		assert yamlConf.dynamicConfiguration.dynamicClientConfig.appId.value.equals("unit_tester");
		assert yamlConf.staticConfiguration.staticClientConfig.maxConnectionsPerNode.value == 10;
	}

	@Test
	public void loadBogusYamlInvalidFormat() {
		String yamlURL = YAML_URL_BASE + BOGUS_YAML_IF_CONF_RELATIVE_PATH;
		System.setProperty("AEROSPIKE_CLIENT_CONFIG_SYS_PROP", yamlURL);
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			YamlConfigProvider yamlLoader = new YamlConfigProvider(yamlURL);
			Configuration yamlConf = yamlLoader.fetchConfiguration();
			assertNull(yamlConf);
		});
		assertTrue(ae.getMessage().contains("Failed to parse"));
	}

	@Test
	public void loadBogusYamlMissingVersion() {
		String yamlURL = YAML_URL_BASE + BOGUS_YAML_MVer_CONF_RELATIVE_PATH;
		System.setProperty("AEROSPIKE_CLIENT_CONFIG_SYS_PROP", yamlURL);
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			YamlConfigProvider yamlLoader = new YamlConfigProvider(yamlURL);
			Configuration yamlConf = yamlLoader.fetchConfiguration();
			assertNull(yamlConf);
		});
		assertTrue(ae.getMessage().contains("Failed to parse"));
		assertTrue(ae.getMessage().contains("YAML config must contain a valid version field."));
	}

	@Test
	public void loadBogusYamlInvalidVersion() {
		String yamlURL = YAML_URL_BASE + BOGUS_YAML_IV_CONF_RELATIVE_PATH;
		System.setProperty("AEROSPIKE_CLIENT_CONFIG_SYS_PROP", yamlURL);
		YamlConfigProvider yamlLoader = new YamlConfigProvider(yamlURL);
		Configuration yamlConf = yamlLoader.fetchConfiguration();
		assertNotNull(yamlConf);
		assertNotNull(yamlConf.dynamicConfiguration.dynamicClientConfig.appId);
		assert yamlConf.dynamicConfiguration.dynamicClientConfig.appId.value.equals("unit_tester");
		assert yamlConf.staticConfiguration.staticClientConfig.maxConnectionsPerNode.value == 99;
	}


	@Test
	public void getStaticYamlConfig() {
		String yamlURL = YAML_URL_BASE + GOOD_YAML_CONF_RELATIVE_PATH;
		System.setProperty("AEROSPIKE_CLIENT_CONFIG_SYS_PROP", yamlURL);
		YamlConfigProvider yamlLoader = new YamlConfigProvider(yamlURL);
		Configuration yamlConf = yamlLoader.fetchConfiguration();
		assertNotNull(yamlConf);
		assertNotNull(yamlConf.staticConfiguration);
		assertNotNull(yamlConf.dynamicConfiguration);
		assert yamlConf.staticConfiguration.staticClientConfig.maxConnectionsPerNode.value == 99;
	}

	@Test
	public void getDynamicYamlConfig() {
		String yamlURL = YAML_URL_BASE + GOOD_YAML_CONF_RELATIVE_PATH;
		System.setProperty("AEROSPIKE_CLIENT_CONFIG_SYS_PROP", yamlURL);
		YamlConfigProvider yamlLoader = new YamlConfigProvider(yamlURL);
		Configuration yamlConf = yamlLoader.fetchDynamicConfiguration();
		assertNotNull(yamlConf);
		assertNull(yamlConf.staticConfiguration);
		assertNotNull(yamlConf.dynamicConfiguration);
		assert yamlConf.dynamicConfiguration.dynamicClientConfig.tendInterval.value == 500;
	}
}
