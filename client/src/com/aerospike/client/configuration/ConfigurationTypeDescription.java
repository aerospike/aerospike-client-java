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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.yaml.snakeyaml.TypeDescription;

import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.*;
import com.aerospike.client.configuration.serializers.StaticConfiguration;
import com.aerospike.client.configuration.serializers.staticconfig.StaticClientConfig;
import com.aerospike.client.configuration.util.YamlFieldNameStrategies;

public class ConfigurationTypeDescription {
    private final Map<Class<?>, TypeDescription> cache;
    private final Set<Class<?>> visited;

    ConfigurationTypeDescription() {
        this.cache = new HashMap<>();
        this.visited = new HashSet<>();
    }

    public Map<Class<?>, TypeDescription> buildTypeDescriptions(String packagePath, Class<?> rootClass) {
        // Temp workaround to prevent snakeyaml logging WARNINGs - should be fixed in snakeyaml 2.5
        java.util.logging.Logger.getLogger("org.yaml.snakeyaml.introspector").setLevel(java.util.logging.Level.SEVERE);
        buildTypeDescriptionsHelper(packagePath, rootClass);
        java.util.logging.Logger.getLogger("org.yaml.snakeyaml.introspector").setLevel(java.util.logging.Level.INFO);
        return cache;
    }

    private void buildTypeDescriptionsHelper(String packagePath, Class<?> clazz) {
        if (visited.contains(clazz)) {
            return;
        }
        visited.add(clazz);
        TypeDescription typeDescription = new TypeDescription(clazz);
        for (Field field : clazz.getDeclaredFields()) {
            Class<?> fieldType = field.getType();

            switch (field.getName()) {
                case "staticConfiguration" ->
                        typeDescription.substituteProperty("static", StaticConfiguration.class, "getStaticConfiguration", "setStaticConfiguration");
                case "staticClientConfig" ->
                        typeDescription.substituteProperty("client", StaticClientConfig.class, "getStaticClientConfig", "setStaticClientConfig");
                case "dynamicConfiguration" ->
                        typeDescription.substituteProperty("dynamic", DynamicConfiguration.class, "getDynamicConfiguration", "setDynamicConfiguration");
                case "dynamicClientConfig" ->
                        typeDescription.substituteProperty("client", DynamicClientConfig.class, "getDynamicClientConfig", "setDynamicClientConfig");
                case "dynamicReadConfig" ->
                        typeDescription.substituteProperty("read", DynamicReadConfig.class, "getDynamicReadConfig", "setDynamicReadConfig");
                case "dynamicWriteConfig" ->
                        typeDescription.substituteProperty("write", DynamicWriteConfig.class, "getDynamicWriteConfig", "setDynamicWriteConfig");
                case "dynamicQueryConfig" ->
                        typeDescription.substituteProperty("query", DynamicQueryConfig.class, "getDynamicQueryConfig", "setDynamicQueryConfig");
                case "dynamicScanConfig" ->
                        typeDescription.substituteProperty("scan", DynamicScanConfig.class, "getDynamicScanConfig", "setDynamicScanConfig");
                case "dynamicBatchReadConfig" ->
                        typeDescription.substituteProperty("batch_read", DynamicBatchReadConfig.class, "getDynamicBatchReadConfig", "setDynamicBatchReadConfig");
                case "dynamicBatchWriteConfig" ->
                        typeDescription.substituteProperty("batch_write", DynamicBatchWriteConfig.class, "getDynamicBatchWriteConfig", "setDynamicBatchWriteConfig");
                case "dynamicBatchUDFconfig" ->
                        typeDescription.substituteProperty("batch_udf", DynamicBatchUDFconfig.class, "getDynamicBatchUDFconfig", "setDynamicBatchUDFconfig");
                case "dynamicBatchDeleteConfig" ->
                        typeDescription.substituteProperty("batch_delete", DynamicBatchDeleteConfig.class, "getDynamicBatchDeleteConfig", "setDynamicBatchDeleteConfig");
                case "dynamicTxnRollConfig" ->
                        typeDescription.substituteProperty("txn_roll", DynamicTxnRollConfig.class, "getDynamicTxnRollConfig", "setDynamicTxnRollConfig");
                case "dynamicTxnVerifyConfig" ->
                        typeDescription.substituteProperty("txn_verify", DynamicTxnVerifyConfig.class, "getDynamicTxnVerifyConfig", "setDynamicTxnVerifyConfig");
                case "dynamicMetricsConfig" ->
                        typeDescription.substituteProperty("metrics", DynamicMetricsConfig.class, "getDynamicMetricsConfig", "setDynamicMetricsConfig");
                default -> {
                    String yamlKey = YamlFieldNameStrategies.camelToSnake(field.getName());
                    String capitalized = field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
                    String getterName = "get" + capitalized;
                    String setterName = "set" + capitalized;
                    typeDescription.substituteProperty(yamlKey, fieldType, getterName, setterName);
                }
            }


            if (isCustomPojo(packagePath, fieldType)) {
                buildTypeDescriptionsHelper(packagePath, fieldType);
            }
        }

        cache.put(clazz, typeDescription);
    }

    private boolean isCustomPojo(String packagePath, Class<?> type) {
        if (type.isPrimitive()) return false; // does not apply to primitives
        if (!type.getName().startsWith(packagePath)) return false; // does not apply to POJO's outside given package path pattern
        return !type.isEnum();
    }
}
