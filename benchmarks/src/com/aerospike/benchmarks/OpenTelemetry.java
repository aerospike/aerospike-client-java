package com.aerospike.benchmarks;

public interface OpenTelemetry extends AutoCloseable {

    public boolean getClosed();
    void addException(Exception exception, LatencyTypes type);
    void recordElapsedTime(String type, long elapsedNanos);
    void recordElapsedTime(LatencyTypes type, long elapsedNanos);
    void incrTransCounter(LatencyTypes type);
    void setClusterName(String clusterName);
    void setDBConnectionState(String dbConnectionState);

    String printConfiguration();
}
