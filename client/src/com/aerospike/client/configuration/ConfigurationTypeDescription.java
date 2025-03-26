package com.aerospike.client.configuration;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicClientConfig;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicReadConfig;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicWriteConfig;
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
        buildTypeDescriptionsHelper(packagePath, rootClass);
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
