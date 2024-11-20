package com.aerospike.benchmarks;

import com.aerospike.client.Host;

public final class OpenTelemetryHelper {

    public static OpenTelemetry Create(int endPointPort,
                                       Arguments args,
                                       Host host,
                                       int hbTimeInterval,
                                       String clusterName,
                                       StringBuilder generalInfo,
                                       StringBuilder policies,
                                       StringBuilder otherInfo,
                                       CounterStore counters) {

        if(endPointPort > 0) {
            return new OpenTelemetryExporter(endPointPort,
                                                args,
                                                host,
                                                hbTimeInterval,
                                                clusterName,
                                                generalInfo,
                                                policies,
                                                otherInfo,
                                                counters);
        }

        return new OpenTelemetryDummy(endPointPort,
                                        args,
                                        host,
                                        hbTimeInterval,
                                        clusterName,
                                        generalInfo,
                                        policies,
                                        otherInfo,
                                        counters);
    }

}
