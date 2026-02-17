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

package com.aerospike.client.configuration.serializers;

import java.util.List;

import com.aerospike.client.Log;
import com.aerospike.client.configuration.YamlConfigProvider;
import com.aerospike.client.configuration.primitiveprops.StringProperty;

public class Configuration {
    public StringProperty version;
    public StaticConfiguration staticConfiguration;
    public DynamicConfiguration dynamicConfiguration;

    public Configuration() {}

    public StringProperty getVersion() {
        return version;
    }

    public void setVersion(StringProperty version) {
        List<String> supportedVersions = YamlConfigProvider.getSupportedVersions();
        if (version == null) {
            Log.error("Empty YAML config schema version. This client supports these schema versions: " +
                    supportedVersions);
        } else if (!supportedVersions.contains(version.value)) {
            if (Log.warnEnabled()) {
                Log.warn("Invalid YAML config schema version " + version.value + ".  This client supports these " +
                        "schema versions: " + supportedVersions);
            }
        }
        this.version = version;
    }

    public StaticConfiguration getStaticConfiguration() {
        return this.staticConfiguration;
    }

    public void setStaticConfiguration(StaticConfiguration staticConfiguration) {
        this.staticConfiguration = staticConfiguration;
    }

    public DynamicConfiguration getDynamicConfiguration() {
        return this.dynamicConfiguration;
    }

    public void setDynamicConfiguration(DynamicConfiguration dynamicConfiguration) {
        this.dynamicConfiguration = dynamicConfiguration;
    }

    public boolean hasMetrics() {
        return dynamicConfiguration != null &&
                dynamicConfiguration.dynamicMetricsConfig != null;
    }

    public boolean hasDBWCsendKey() {
		return dynamicConfiguration != null &&
				dynamicConfiguration.dynamicBatchWriteConfig != null &&
				dynamicConfiguration.dynamicBatchWriteConfig.sendKey != null;
	}

    public boolean hasDBUDFCsendKey() {
        return dynamicConfiguration != null &&
                dynamicConfiguration.dynamicBatchUDFconfig != null &&
                dynamicConfiguration.dynamicBatchUDFconfig.sendKey != null;
    }

    public boolean hasDBDCsendKey() {
        return dynamicConfiguration != null &&
                dynamicConfiguration.dynamicBatchDeleteConfig != null &&
                dynamicConfiguration.dynamicBatchDeleteConfig.sendKey != null;
    }

    public String getAppID() {
        if (dynamicConfiguration.dynamicClientConfig.getAppId() != null) {
            return dynamicConfiguration.dynamicClientConfig.appId.value;
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            if (getVersion() != null) {
                propsString.append("\n\tversion= ").append(getVersion().value);
            }
            propsString.append("\n\tstatic= ").append(getStaticConfiguration());
            propsString.append("\n\tdynamic= ").append(getDynamicConfiguration());
            propsString.append("\n");
        } catch (Exception e) {
            if (Log.warnEnabled()) {
                Log.warn(e.toString());
            }
        }
        return propsString.toString();
    }
}
