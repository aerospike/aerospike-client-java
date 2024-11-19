package com.aerospike.benchmarks;

public interface OpenTelemetry extends AutoCloseable {

    public boolean getClosed();
    void addException(Exception exception, LatencyTypes type);
    void recordElapsedTime(LatencyTypes type, long elapsedMS);
    void incrTransCounter(LatencyTypes type);
    void setClusterName(String clusterName);
    void setDBConnectionState(String dbConnectionState);

    String printConfiguration();
}
