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
import com.aerospike.client.configuration.primitiveprops.ByteSizeProperty;
import com.aerospike.client.configuration.primitiveprops.DurationProperty;
import com.aerospike.client.configuration.primitiveprops.IntProperty;
import com.aerospike.client.configuration.primitiveprops.StringProperty;
import com.aerospike.client.Log;

/**
 * YAML model for legacy flat metrics keys and the nested v3 metrics settings.
 */
public class DynamicMetricsConfig {
	/** Legacy flat metrics enable flag. */
    public BooleanProperty enable;

	/** Legacy flat latency histogram shift. */
    public IntProperty latencyShift;

	/** Legacy flat latency histogram column count. */
    public IntProperty latencyColumns;

	/** Nested metrics enable flag. */
	public BooleanProperty enabled;

	/** Elapsed time between snapshot exports. */
	public DurationProperty exportInterval;

	/** FILE, CUSTOM or NONE exporter selection. */
	public StringProperty exporter;

	/** Directory used by the FILE exporter. */
	public StringProperty reportDir;

	/** FILE exporter rotation limit in bytes. */
	public ByteSizeProperty reportSizeLimit;

	/** Labels attached to exported snapshots. */
    public Map<String, String> labels;

	/** Nested operational and usage collection settings. */
	public DynamicMetricsExtendedConfig extended;

	/** Initialize an empty model for YAML deserialization. */
    public DynamicMetricsConfig() {}

	/** Set the legacy flat metrics enable flag. */
    public void setEnable(BooleanProperty enable) { this.enable = enable; }

	/** Set the legacy flat latency histogram shift. */
    public void setLatencyShift(IntProperty latencyShift) { this.latencyShift = latencyShift; }

	/** Set the legacy flat latency histogram column count. */
    public void setLatencyColumns(IntProperty latencyColumns) { this.latencyColumns = latencyColumns; }

	/** Return configured snapshot labels. */
    public Map<String, String> getLabels() { return labels; }

	/** Set snapshot labels. */
    public void setLabels(Map<String, String> labels) { this.labels = labels; }

	/** Return the legacy flat metrics enable flag. */
    public BooleanProperty getEnable() { return enable; }

	/** Return the legacy flat latency histogram shift. */
    public IntProperty getLatencyShift() { return latencyShift; }

	/** Return the legacy flat latency histogram column count. */
    public IntProperty getLatencyColumns() { return latencyColumns; }

	/** Return the nested metrics enable flag. */
	public BooleanProperty getEnabled() { return enabled; }

	/** Set the nested metrics enable flag. */
	public void setEnabled(BooleanProperty enabled) { this.enabled = enabled; }

	/** Return the elapsed snapshot export interval. */
	public DurationProperty getExportInterval() { return exportInterval; }

	/** Set the elapsed snapshot export interval. */
	public void setExportInterval(DurationProperty exportInterval) { this.exportInterval = exportInterval; }

	/** Return the configured exporter selection. */
	public StringProperty getExporter() { return exporter; }

	/** Set the exporter selection. */
	public void setExporter(StringProperty exporter) { this.exporter = exporter; }

	/** Return the FILE exporter directory. */
	public StringProperty getReportDir() { return reportDir; }

	/** Set the FILE exporter directory. */
	public void setReportDir(StringProperty reportDir) { this.reportDir = reportDir; }

	/** Return the FILE exporter rotation limit. */
	public ByteSizeProperty getReportSizeLimit() { return reportSizeLimit; }

	/** Set the FILE exporter rotation limit. */
	public void setReportSizeLimit(ByteSizeProperty reportSizeLimit) { this.reportSizeLimit = reportSizeLimit; }

	/** Return nested operational and usage settings. */
	public DynamicMetricsExtendedConfig getExtended() { return extended; }

	/** Set nested operational and usage settings. */
	public void setExtended(DynamicMetricsExtendedConfig extended) { this.extended = extended; }


    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" enable=").append(enable.value).append(", ");
            propsString.append(" latency_shift=").append(latencyShift.value).append(", ");
            propsString.append(" latency_columns=").append(latencyColumns.value).append(", ");
			propsString.append(" enabled=").append(enabled != null ? enabled.value : "null").append(", ");
			propsString.append(" export_interval=").append(exportInterval != null ? exportInterval.value : "null").append(", ");
			propsString.append(" exporter=").append(exporter != null ? exporter.value : "null").append(", ");
			propsString.append(" report_dir=").append(reportDir != null ? reportDir.value : "null").append(", ");
			propsString.append(" report_size_limit=").append(reportSizeLimit != null ? reportSizeLimit.value : "null").append(", ");
            propsString.append(" labels=").append(getLabels().toString()).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.append("}").toString();
    }
}
