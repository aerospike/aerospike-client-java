package com.aerospike.client.configuration.serializers.dynamicconfig;


import com.aerospike.client.Log;
import com.aerospike.client.configuration.serializers.dynamicconfig.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.serializers.dynamicconfig.primitiveprops.IntProperty;

public class DynamicMetricsConfig {
    public BooleanProperty enable;
    public IntProperty latencyShift;
    public IntProperty latencyColumns;

    public DynamicMetricsConfig() {}

    public void setEnable(BooleanProperty enable) { this.enable = enable; }

    public void setLatencyShift(IntProperty latencyShift) { this.latencyShift = latencyShift; }

    public void setLatencyColumns(IntProperty latencyColumns) { this.latencyColumns = latencyColumns; }

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
        } catch (Exception e) {
            Log.error(e.toString());
        } finally {
            return propsString.append("}").toString();
        }
    }
}
