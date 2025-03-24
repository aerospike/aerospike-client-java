/*
 * Copyright 2012-2024 Aerospike, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;

import com.aerospike.client.policy.ClientPolicy;
import org.junit.Test;

import com.aerospike.client.configuration.YamlConfigProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.test.sync.TestSync;

public class TestConfigLoadYAML extends TestSync {
    public static String goodYamlConfRelativePath = "/src/resources/aerospikeconfig.yaml";
    public static String bogusYamlIPconfRelativePath = "/src/resources/bogus_invalid_property.yaml";
    public static String bogusYamlIFconfRelativePath = "/src/resources/bogus_invalid_format.yaml";
    @Test
    public void loadGoodYAML() throws IOException, InterruptedException {
        YamlConfigProvider yamlLoader = new YamlConfigProvider(System.getProperty("user.dir") + goodYamlConfRelativePath);
        Configuration yamlConf = yamlLoader.fetchConfiguration();
        assert yamlConf != null;
        System.out.println(yamlConf);
        assert yamlConf.metadata.appName.equals("example_app");
        assert yamlConf.staticConfiguration.staticClientConfig.maxConnectionsPerNode.value == 99;
        Thread.sleep(3000);
    }

    @Test
    public void loadBogusIPYAML() throws IOException, InterruptedException {
        YamlConfigProvider yamlLoader = new YamlConfigProvider(System.getProperty("user.dir") + bogusYamlIPconfRelativePath);
        Configuration yamlConf = yamlLoader.fetchConfiguration();
        assert yamlConf == null;
    }

    @Test
    public void loadBogusMVYAML() throws IOException, InterruptedException {
        YamlConfigProvider yamlLoader = new YamlConfigProvider(System.getProperty("user.dir") + bogusYamlIFconfRelativePath);
        Configuration yamlConf = yamlLoader.fetchConfiguration();
        assert yamlConf == null;
    }

    @Test
    public void testPolicyConfig() {
        var clientPolicy = new ClientPolicy();
        clientPolicy.configProvider = new YamlConfigProvider(System.getProperty("user.dir") + goodYamlConfRelativePath);
        assert clientPolicy.configProvider.fetchConfiguration() != null;

    }

    @Test
    public void getStaticYAMLmap() {
        YamlConfigProvider yamlLoader = new YamlConfigProvider(System.getProperty("user.dir") + goodYamlConfRelativePath);
        Configuration yamlConf = yamlLoader.fetchConfiguration();
        assert yamlConf != null;
        assert yamlConf.staticConfiguration.staticClientConfig.configInterval.value == 3;
    }

    @Test
    public void getDynamicYAMLmap() {
        YamlConfigProvider yamlLoader = new YamlConfigProvider(System.getProperty("user.dir") + goodYamlConfRelativePath);
        Configuration yamlConf = yamlLoader.fetchConfiguration();
        assert yamlConf != null;
    }

    @Test
    public void monitorYAML() {

    }
}