package com.aerospike.benchmarks;

import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.*;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class OpenTelemetryExporter implements com.aerospike.benchmarks.OpenTelemetry {

    private final String SCOPE_NAME = "com.aerospike.benchmarks";
    private final String METRIC_NAME = "aerospike.benchmarks.mrt";

    private final int prometheusPort;
    private String clusterName = null;
    private String dbConnectionState = null;
    private final CounterStore counters;

    private final OpenTelemetrySdk openTelemetrySdk;
    private final Meter openTelemetryMeter;
    private final ObservableLongGauge openTelemetryHBGaugeCB;
    private final LongCounter openTelemetryExceptionCounter;
    private final LongCounter openTelemetryTransactionCounter;
    private final LongHistogram openTelemetryLatencyHistogram;

    private long hbCnt = 0;
    private final long startTimeMillis;
    private final LocalDateTime startLocalDateTime;
    private final boolean debug;
    private boolean closed = false;

    private final Timer hbTimer;

    private AtomicLong writeCounter = new AtomicLong();
    private AtomicLong writeError = new AtomicLong();
    private AtomicLong writeTimeout = new AtomicLong();
    private AtomicLong readCounter = new AtomicLong();
    private AtomicLong readError = new AtomicLong();
    private AtomicLong readTimeout = new AtomicLong();
    private AtomicLong txnCounter = new AtomicLong();
    private AtomicLong txnError = new AtomicLong();
    private AtomicLong txnTimeout = new AtomicLong();

    public OpenTelemetryExporter(int prometheusPort,
                                 Arguments args,
                                 Host host,
                                 String clusterName,
                                 StringBuilder generalInfo,
                                 StringBuilder policies,
                                 StringBuilder otherInfo,
                                 CounterStore counters) {
        this.debug = args.debug;
        this.clusterName = clusterName;
        this.startTimeMillis = System.currentTimeMillis();
        this.startLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(this.startTimeMillis),
                                    ZoneId.systemDefault());
        this.prometheusPort = prometheusPort;
        this.counters = counters;

        if(this.debug) {
            this.printDebug("Creating OpenTelemetryExporter");
        }

        this.openTelemetrySdk = this.initOpenTelemetry();
        this.openTelemetryMeter = this.openTelemetrySdk.getMeter(SCOPE_NAME);

        final Attributes attributes1 = Attributes.of(
                AttributeKey.stringKey("generalInfo"), generalInfo.toString(),
                AttributeKey.stringKey("policies"), policies.toString(),
                AttributeKey.stringKey("otherInfo"), otherInfo.toString(),
                AttributeKey.longKey("startTimeMillis"), this.startTimeMillis,
                AttributeKey.stringKey("startDateTime"), this.startLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        );

        final Attributes attributes2 = Attributes.of(
                AttributeKey.stringKey("DBHost"), host.toString(),
                AttributeKey.stringArrayKey("namespaces"), args.batchSize > 0 ? Arrays.asList(args.batchNamespaces) : Collections.singletonList(args.namespace),
                AttributeKey.stringKey("namespace"), args.namespace == null ? args.batchNamespaces[0] : args.namespace,
                AttributeKey.stringKey("setname"), args.setName,
                AttributeKey.stringKey("workload"), args.workload.name(),
                AttributeKey.longKey("batchSize"), (long) args.batchSize
        );

         this.hbTimer = new Timer();
         
        if(this.debug) {
            this.printDebug("Creating Metrics");
        }

        this.openTelemetryHBGaugeCB =
                openTelemetryMeter
                    .gaugeBuilder(METRIC_NAME + ".heartbeat")
                    .ofLongs()
                    .setDescription("Aerospike Benchmark MRT HB")
                    .buildWithCallback(hb -> {
                        AttributesBuilder attributes = Attributes.builder();

                        attributes.putAll(attributes1);
                        attributes.putAll(attributes2);

                        long writeCounter = this.writeCounter.get();
                        long writeError = this.writeError.get();
                        long writeTimeout = this.writeTimeout.get();
                        long readCounter = this.readCounter.get();
                        long readError = this.readError.get();
                        long readTimeout = this.readTimeout.get();
                        long txnCounter = this.txnCounter.get();
                        long txnError = this.txnError.get();
                        long txnTimeout = this.txnTimeout.get();

                        attributes.put("writecounts", writeCounter);
                        attributes.put("writetimeoutcount", writeTimeout);
                        attributes.put("writeerrorcount", writeError);
                        attributes.put("readcounts", readCounter);
                        attributes.put("readtimeoutcount", readTimeout);
                        attributes.put("readerrorcount", readError);
                        attributes.put("txncounts", txnCounter);
                        attributes.put("txntimeoutcount", txnTimeout);
                        attributes.put("txnerrorcount", txnError);
                        attributes.put("counts", writeCounter + readCounter + txnCounter);
                        attributes.put("timeoutcount", writeTimeout + readTimeout + txnTimeout);
                        attributes.put("errorcount", writeError + readError + txnError);
                        if(this.clusterName != null) {
                            attributes.put("cluster", this.clusterName);
                        }
                        if(this.dbConnectionState != null) {
                            attributes.put("DBConnState", this.dbConnectionState);
                        }

                        hb.record(this.hbCnt++, attributes.build());

                        //if(this.debug) {
                            this.printDebug(String.format("HeartBeat %d %d", hbCnt, writeCounter + readCounter + txnCounter));
                        //}
                    });

        this.openTelemetryExceptionCounter =
                openTelemetryMeter
                    .counterBuilder(METRIC_NAME + ".exception")
                        .setDescription("Aerospike Benchmark MRT Exception")
                        .build();

        this.openTelemetryLatencyHistogram =
                openTelemetryMeter
                        .histogramBuilder(METRIC_NAME + ".latency")
                        .setDescription("Aerospike Benchmark MRT Latencies")
                        .ofLongs()
                        .setUnit("ms")
                        .build();

        this.openTelemetryTransactionCounter =
                openTelemetryMeter
                        .counterBuilder(METRIC_NAME + ".transaction")
                        .setDescription("Aerospike Benchmark MRT Transaction")
                        .build();

        if(this.debug) {
            this.printDebug("SDK and Metrics Completed");
            this.printDebug("Register to Counters");
        }

        this.counters.setOpenTelemetry(this);

    }

    private OpenTelemetrySdk initOpenTelemetry() {
        // Include required service.name resource attribute on all spans and metrics
        if(this.debug) {
            this.printDebug("Creating SDK");
        }
        Resource resource =
                Resource.getDefault()
                        .merge(Resource.builder().put(SERVICE_NAME, "PrometheusExporterAerospikeMRT").build());

        return OpenTelemetrySdk.builder()
                /*.setTracerProvider(
                        SdkTracerProvider.builder()
                                .setResource(resource)
                                .addSpanProcessor(SimpleSpanProcessor.create(LoggingSpanExporter.create()))
                                .build())*/
                .setMeterProvider(
                        SdkMeterProvider.builder()
                                .setResource(resource)
                                .registerMetricReader(
                                        PrometheusHttpServer.builder().setPort(prometheusPort).build())
                                .build())
                .buildAndRegisterGlobal();
    }

    private final DateTimeFormatter debugDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final AtomicInteger debugCnt = new AtomicInteger(0);
    private void printDebug(String msg, boolean limited) {

        if(limited) {
            if(debugCnt.incrementAndGet() <= 1) {
                printDebug("LIMIT1 " + msg);
            }
            else if(debugCnt.compareAndSet(100, 1)){
                printDebug("LIMIT100 " + msg);
            }
        }
        else {
            printDebug(msg);
        }
    }
    private void printDebug(String msg) {
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(debugDateFormatter);

        System.out.printf("%s DEBUG OTEL %s%n", formattedDateTime, msg);
    }

    @Override
    public void addException(Exception exception, LatencyTypes type) {

        final Attributes attributes = Attributes.of(
                AttributeKey.stringKey("exception_type"), exception.getClass().getName(),
                AttributeKey.stringKey("exception"), exception.getMessage(),
                AttributeKey.stringKey("type"), type.name().toLowerCase()
        );

        this.openTelemetryExceptionCounter.add(1, attributes);

        if(exception instanceof AerospikeException.Timeout) {
            switch (type) {
                case TRANSACTION -> this.txnTimeout.incrementAndGet();
                case READ -> this.readTimeout.incrementAndGet();
                case WRITE -> this.writeTimeout.incrementAndGet();
                default -> this.printDebug("Exception Counter Type unknown " + type, true);
            }
        }
        else {
            switch (type) {
                case TRANSACTION -> this.txnError.incrementAndGet();
                case READ -> this.readError.incrementAndGet();
                case WRITE -> this.writeError.incrementAndGet();
                default -> this.printDebug("Exception Counter Type unknown " + type, true);
            }
        }

        if(this.debug) {
            this.printDebug("Exception Counter Add " + attributes.get(AttributeKey.stringKey("exception_type")));
        }
    }

    @Override
    public void recordElapsedTime(LatencyTypes type, long elapsedMS) {

        final Attributes attributes = Attributes.of(
            AttributeKey.stringKey("type"), type.name().toLowerCase()
        );

        this.openTelemetryLatencyHistogram.record(elapsedMS, attributes);
        this.incrTransCounter(type, attributes);

        if(this.debug) {
            this.printDebug("Elapsed Time Record  " + attributes.get(AttributeKey.stringKey("type")), true);
        }
    }

    private void incrTransCounter(LatencyTypes type, Attributes attributes) {

        this.openTelemetryTransactionCounter.add(1, attributes);

        switch (type) {
            case TRANSACTION -> this.txnCounter.incrementAndGet();
            case READ -> this.readCounter.incrementAndGet();
            case WRITE -> this.writeCounter.incrementAndGet();
            default -> this.printDebug("Transaction Counter Type unknown " + type, true);
        }
    }

    @Override
    public void incrTransCounter(LatencyTypes type) {

        final Attributes attributes = Attributes.of(
                AttributeKey.stringKey("type"), type.name().toLowerCase()
        );

        this.incrTransCounter(type, attributes);

        if (this.debug) {
            this.printDebug("Transaction Counter Add " + type.name().toLowerCase(), true);
        }
    }

    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
        if(this.debug) {
            this.printDebug("Cluster Name  " + clusterName, false);
        }
    }

    @Override
    public void setDBConnectionState(String dbConnectionState){
        this.dbConnectionState = dbConnectionState;
    }

    @Override
    public boolean getClosed() { return this.closed; }

    @Override
    public void close() throws Exception {

        if(this.debug) {
            this.printDebug("Closing");
        }

        closed = true;

        if(this.counters != null) {
            this.counters.setOpenTelemetry(null);
        }

        if(openTelemetryHBGaugeCB != null) {
            openTelemetryHBGaugeCB.close();
        }
        if(openTelemetrySdk != null) {
            this.openTelemetrySdk.close();
        }

        if(this.debug) {
            this.printDebug("Closed");
        }
    }

    public String printConfiguration() {
        return String.format("Open Telemetry Enabled at %s\n\tPrometheus Exporter using Port %d\n\tScope Name: '%s'\n\tMetric Prefix Name: '%s'",
                this.startLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                this.prometheusPort,
                SCOPE_NAME,
                METRIC_NAME);
    }

    @Override
    public String toString() {
        return String.format("OpenTelemetryExporter{prometheusport:%d, heartbeat:%s, exceptioncounter:%s, latancyhistogram:%s}",
                                this.prometheusPort,
                                this.openTelemetryHBGaugeCB,
                                this.openTelemetryExceptionCounter,
                                this.openTelemetryLatencyHistogram);
    }
}
