package com.aerospike.benchmarks;

import com.aerospike.client.Host;

public class OpenTelemetryDummy  implements OpenTelemetry {

    @SuppressWarnings("unused")
    public OpenTelemetryDummy(int endPointPort,
                                Arguments args,
                                Host host,
                                int closeWaitMS,
                                String clusterName,
                                StringBuilder generalInfo,
                                StringBuilder policies,
                                StringBuilder otherInfo,
                                long nKeys,
                                int nthreads,
                                long keysMRT,
                                boolean asyncEnabled,
                                CounterStore counters) {
        counters.setOpenTelemetry(this);
    }

    @Override
    public boolean getClosed() { return false; }

    @SuppressWarnings("unused")
    @Override
    public void incrTransCounter(LatencyTypes type) {
    }

    @SuppressWarnings("unused")
    @Override
    public void addException(Exception exception, LatencyTypes type) {
    }

    @SuppressWarnings("unused")
    @Override
    public void recordElapsedTime(String type, long elapsedNanos) {
    }

    @SuppressWarnings("unused")
    @Override
    public void recordElapsedTime(LatencyTypes type, long elapsedNanos) {
    }

    @Override
    public void close() throws Exception {
    }

    @SuppressWarnings("unused")
    @Override
    public void setClusterName(String clusterName) {
    }

    @SuppressWarnings("unused")
    @Override
    public void setDBConnectionState(String dbConnectionState){
    }

    @Override
    public String printConfiguration() {
        return "Open Telemetry Disabled";
    }

    @Override
    public String toString() {
        return "OpenTelemetryDummy{}";
    }
}
