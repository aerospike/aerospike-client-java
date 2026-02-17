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

package com.aerospike.client.configuration.serializers.dynamicconfig;

import java.util.Map;

import com.aerospike.client.configuration.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.primitiveprops.IntProperty;
import com.aerospike.client.Log;

public class DynamicMetricsConfig {
    public BooleanProperty enable;
    public IntProperty latencyShift;
    public IntProperty latencyColumns;
    public Map<String, String> labels;

    public DynamicMetricsConfig() {}

    public void setEnable(BooleanProperty enable) { this.enable = enable; }

    public void setLatencyShift(IntProperty latencyShift) { this.latencyShift = latencyShift; }

    public void setLatencyColumns(IntProperty latencyColumns) { this.latencyColumns = latencyColumns; }

    public Map<String, String> getLabels() { return labels; }

    public void setLabels(Map<String, String> labels) { this.labels = labels; }

    public BooleanProperty getEnable() { return enable; }

    public IntProperty getLatencyShift() { return latencyShift; }

    public IntProperty getLatencyColumns() { return latencyColumns; }


    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" enable=").append(enable.value).append(", ");
            propsString.append(" latency_shift=").append(latencyShift.value).append(", ");
            propsString.append(" latency_columns=").append(latencyColumns.value).append(", ");
            propsString.append(" labels=").append(getLabels().toString()).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.append("}").toString();
    }
}
