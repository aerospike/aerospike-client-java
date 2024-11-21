package com.aerospike.benchmarks;

import com.aerospike.client.Host;

public final class OpenTelemetryHelper {

    public static OpenTelemetry Create(int endPointPort,
                                       Arguments args,
                                       Host host,
                                       boolean enable,
                                       String clusterName,
                                       StringBuilder generalInfo,
                                       StringBuilder policies,
                                       StringBuilder otherInfo,
                                       long nKeys,
                                       int nThreads,
                                       long keysMRT,
                                       boolean asyncEnabled,
                                       CounterStore counters) {

        if(enable) {
            return new OpenTelemetryExporter(endPointPort,
                                                args,
                                                host,
                                                clusterName,
                                                generalInfo,
                                                policies,
                                                otherInfo,
                                                nKeys,
                                                nThreads,
                                                keysMRT,
                                                asyncEnabled,
                                                counters);
        }

        return new OpenTelemetryDummy(endPointPort,
                                        args,
                                        host,
                                        clusterName,
                                        generalInfo,
                                        policies,
                                        otherInfo,
                                        nKeys,
                                        nThreads,
                                        keysMRT,
                                        asyncEnabled,
                                        counters);
    }

}
