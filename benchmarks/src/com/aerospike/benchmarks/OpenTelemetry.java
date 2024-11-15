package com.aerospike.benchmarks;

public interface OpenTelemetry extends AutoCloseable {

    public boolean getClosed();
    void addException(Exception exception);
    void recordElapsedTime(LatencyTypes type, long elapsed, boolean isMicroSeconds);
    void incrTransCounter(LatencyTypes type);

    String printConfiguration();
}
