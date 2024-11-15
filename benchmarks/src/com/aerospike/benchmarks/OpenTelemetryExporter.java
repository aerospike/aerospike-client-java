package com.aerospike.benchmarks;

import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;

import com.aerospike.client.Host;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.*;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class OpenTelemetryExporter implements com.aerospike.benchmarks.OpenTelemetry {

    private final String SCOPE_NAME = "com.aerospike.benchmarks";
    private final String METRIC_NAME = "aerospike.benchmarks.mrt";

    private final int prometheusPort;
    private final CounterStore counters;

    private final OpenTelemetrySdk openTelemetrySdk;
    private final Meter openTelemetryMeter;
    private final ObservableLongGauge openTelemetryHBGauge;
    private final LongCounter openTelemetryExceptionCounter;
    private final LongHistogram openTelemetryLatencyHistogram;

    private long hbCnt = 0;
    private final long startTimeMillis;
    private final LocalDateTime startLocalDateTime;
    private final boolean debug;
    private boolean closed = false;

    public OpenTelemetryExporter(int prometheusPort,
                                 Arguments args,
                                 Host host,
                                 StringBuilder generalInfo,
                                 StringBuilder policies,
                                 StringBuilder otherInfo,
                                 CounterStore counters) {
        this.debug = args.debug;
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
                AttributeKey.stringKey("setname"), args.setName,
                AttributeKey.stringKey("workload"), args.workload.name(),
                AttributeKey.longKey("batchSize"), (long) args.batchSize
        );

        if(this.debug) {
            this.printDebug("Creating Metrics");
        }

        this.openTelemetryHBGauge =
                openTelemetryMeter
                    .gaugeBuilder(METRIC_NAME + ".heartbeat")
                    .ofLongs()
                    .setDescription("Aerospike Benchmark MRT HB")
                    .buildWithCallback(hb -> {

                        // cunt, timeouts, errors
                        //Elements 0-2 Writes, 3-5 Reads, 6-8 Transactions, 9-11 totals
                        long[] counts = this.counters.getCounts();

                        AttributesBuilder attributes = Attributes.builder();

                        attributes.putAll(attributes1);
                        attributes.putAll(attributes2);
                        attributes.put("writecounts", counts[0]);
                        attributes.put("writetimeoutcount", counts[1]);
                        attributes.put("writeerrorcount", counts[2]);
                        attributes.put("readcounts", counts[3]);
                        attributes.put("readtimeoutcount", counts[4]);
                        attributes.put("readerrorcount", counts[5]);
                        attributes.put("txncounts", counts[6]);
                        attributes.put("txntimeoutcount", counts[7]);
                        attributes.put("txnerrorcount", counts[8]);
                        attributes.put("counts", counts[9]);
                        attributes.put("timeoutcount", counts[10]);
                        attributes.put("errorcount", counts[11]);

                        hb.record(this.hbCnt++, attributes.build());

                        if(this.debug) {
                            this.printDebug("HeartBeat");
                        }
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
                        .setUnit(counters.showMicroSeconds ? "us" : "ms")
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
    public void addException(Exception exception) {

        final Attributes attributes = Attributes.of(
                AttributeKey.stringKey("exception_type"), exception.getClass().getName()
        );

        this.openTelemetryExceptionCounter.add(1, attributes);

        if(this.debug) {
            this.printDebug("Exception Counter Add " + attributes.get(AttributeKey.stringKey("exception_type")));
        }
    }

    @Override
    public void recordElapsedTime(LatencyTypes type, long elapsed, boolean isMicroSeconds) {

        final Attributes attributes = Attributes.of(
            AttributeKey.stringKey("type"), type.name().toLowerCase()
        );

        this.openTelemetryLatencyHistogram.record(elapsed, attributes);

        if(this.debug) {
            this.printDebug("Elapsed Time Counter Add " + attributes.get(AttributeKey.stringKey("type")), true);
        }
    }

    @Override
    public boolean getClosed() { return this.closed; }

    @Override
    public void close() throws Exception {

        if(this.debug) {
            this.printDebug("Closing");
        }

        closed = true;

        if(openTelemetryMeter != null) {
            openTelemetryHBGauge.close();
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
                                this.openTelemetryHBGauge,
                                this.openTelemetryExceptionCounter,
                                this.openTelemetryLatencyHistogram);
    }
}
