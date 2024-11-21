package com.aerospike.benchmarks;

import com.aerospike.client.Host;

public class OpenTelemetryDummy  implements OpenTelemetry {

    @SuppressWarnings("unused")
    public OpenTelemetryDummy(int endPointPort,
                                Arguments args,
                                Host host,
                                String clusterName,
                                StringBuilder generalInfo,
                                StringBuilder policies,
                                StringBuilder otherInfo,
                                long nKeys,
                                int nthreads,
                                long nbrMRTs,
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
    public void recordElapsedTime(LatencyTypes type, double elapsedMS) {
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
