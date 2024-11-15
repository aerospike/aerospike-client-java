package com.aerospike.benchmarks;

import com.aerospike.client.Host;

public class OpenTelemetryDummy  implements OpenTelemetry {

    @SuppressWarnings("unused")
    public OpenTelemetryDummy(int endPointPort,
                                 Arguments args,
                                 Host host,
                                 StringBuilder generalInfo,
                                 StringBuilder policies,
                                 StringBuilder otherInfo,
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
    public void addException(Exception exception) {
    }

    @SuppressWarnings("unused")
    @Override
    public void recordElapsedTime(LatencyTypes type, long elapsed, boolean isMicroSeconds) {
    }

    @Override
    public void close() throws Exception {
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
