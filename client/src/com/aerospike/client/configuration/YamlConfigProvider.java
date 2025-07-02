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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.configuration.serializers.Configuration;

public class YamlConfigProvider implements ConfigurationProvider {
	private static final String CONFIG_PATH_ENV = "AEROSPIKE_CLIENT_CONFIG_URL";
	private static final String CONFIG_PATH_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
	private static final String YAML_SERIALIZERS_PATH = "com.aerospike.client.configuration.serializers.";
	private static final String DEFAULT_CONFIG_URL_PREFIX = "file://";
	private static final List<String> SUPPORTED_SCHEMA_VERSIONS = Collections.singletonList("1.0.0");

	public static List<String> getSupportedVersions() {
		return SUPPORTED_SCHEMA_VERSIONS;
	}

	public static String getConfigPath() {
		String configPath = System.getenv(CONFIG_PATH_ENV);

		if (configPath == null) {
			// System property CONFIG_PATH_SYS_PROP is only intended to be used for testing.
			configPath = System.getProperty(CONFIG_PATH_SYS_PROP);
		}
		return configPath;
	}

	public static YamlConfigProvider getConfigProvider(String configPath) {
		try {
			return new YamlConfigProvider(configPath);
		}
		catch (Exception e) {
			if (Log.warnEnabled()) {
				Log.warn(e.getMessage());
			}
			return null;
		}
	}

	private String path;
	private Configuration configuration;
	private long lastModified;

	public YamlConfigProvider(String configPath) {
		if (Log.debugEnabled()) {
			Log.debug("Supported YAML config schema versions: " +  getSupportedVersions());
		}
		try {
			if (!configPath.startsWith(DEFAULT_CONFIG_URL_PREFIX)) {
				configPath = DEFAULT_CONFIG_URL_PREFIX + configPath;
			}

			URI envURI = new URI(configPath);
			this.path = Paths.get(envURI).toString();
			loadConfiguration();
		}
		catch (Throwable t) {
			throw new AerospikeException("Failed to parse " + configPath + ": " + t);
		}
	}

	public Configuration fetchConfiguration() {
		return configuration;
	}

	public Configuration fetchDynamicConfiguration() {
		if (configuration.staticConfiguration != null) {
		   configuration.staticConfiguration = null;
		}
		return configuration;
	}

	/**
	 * Attempt to load a YAML configuration from the configuration path.
	 * @return True if a YAML config file could be loaded and parsed
	 */
	public boolean loadConfiguration() {
		File file = new File(path);

		if (!file.exists()) {
			throw new AerospikeException("File does not exist: " + path);
		}

		long newLastModified = file.lastModified();

		if (newLastModified == lastModified) {
			return false;
		}

		ConfigurationTypeDescription configurationTypeDescription = new ConfigurationTypeDescription();
		LoaderOptions yamlLoaderOptions = new LoaderOptions();
		Map<Class<?>, TypeDescription> typeDescriptions = configurationTypeDescription.buildTypeDescriptions(YAML_SERIALIZERS_PATH, Configuration.class);
		Constructor typeDescriptionConstructor = new Constructor(Configuration.class, yamlLoaderOptions);
		typeDescriptions.values().forEach(typeDescriptionConstructor::addTypeDescription);
		DumperOptions dumperOptions = new DumperOptions();
		Representer representer = new Representer(dumperOptions);
		representer.getPropertyUtils().setSkipMissingProperties(true);
		Yaml yaml = new Yaml(typeDescriptionConstructor, representer, dumperOptions);
		FileInputStream fileInputStream;

		try {
			fileInputStream = new FileInputStream(path);
		}
		catch (FileNotFoundException e) {
			throw new AerospikeException(e);
		}

		try {
			configuration = yaml.load(fileInputStream);
			lastModified = newLastModified;

			if ( configuration.getVersion() == null ) {
				throw new AerospikeException("YAML config must contain a valid version field.");
			}
			if (Log.debugEnabled()) {
				Log.debug("YAML config successfully loaded.");
			}
		}
		finally {
			try {
				fileInputStream.close();
			}
			catch (Throwable t) {
				// Ignore stream close errors.
			}
		}
		return true;
	}
}
