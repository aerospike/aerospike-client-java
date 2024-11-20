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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public final class OpenTelemetryExporter implements com.aerospike.benchmarks.OpenTelemetry {

    private final String SCOPE_NAME = "com.aerospike.benchmarks";
    private final String METRIC_NAME = "aerospike.benchmarks.mrt";

    private final int prometheusPort;
    private String clusterName;
    private String dbConnectionState = null;
    private final CounterStore counters;
    private final int hbTimeInterval;

    private final Attributes[] hbAttributes;
    private final OpenTelemetrySdk openTelemetrySdk;
    private final LongGauge openTelemetryHBGauge;
    private final LongCounter openTelemetryExceptionCounter;
    private final LongCounter openTelemetryTransactionCounter;
    private final LongHistogram openTelemetryLatencyHistogram;

    private AtomicInteger hbCnt = new AtomicInteger();
    private final long startTimeMillis;
    private final LocalDateTime startLocalDateTime;
    private long endTimeMillis;
    private LocalDateTime endLocalDateTime;
    private final boolean debug;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean aborted = new AtomicBoolean(false);

    private final Timer hbTimer;
    private final HBTimerTask hbTimerTask;

    private static class HBTimerTask extends TimerTask {

        final OpenTelemetryExporter exporter;

        public HBTimerTask(OpenTelemetryExporter exporter) {
            this.exporter = exporter;
        }

        @Override
        public void run() {
            this.exporter.updateHBGauge();
        }
    }

    //If a signal is sent, this will help ensure proper shutdown of OpenTel
    private static class OTelSignalHandler implements SignalHandler {

        final OpenTelemetryExporter exporter;
        boolean handlerRunning = false;
        final SignalHandler oldHandler;

        public OTelSignalHandler(OpenTelemetryExporter exporter) {
            this.exporter = exporter;

            // First register with SIG_DFL, just to get the old handler.
            Signal sigInt = new Signal("INT");
            oldHandler = Signal.handle(sigInt, SignalHandler.SIG_DFL );
        }

        @Override
        public void handle(Signal sig) {
            if(this.handlerRunning) {return;}
            this.handlerRunning = true;
            this.exporter.printMsg("Received signal: " + sig.getName());
            try {
                this.exporter.setDBConnectionState("SIG" + sig.getName());
                this.exporter.aborted.set(true);
                Thread.sleep(1000);
                this.exporter.close();
            }
            catch (Exception ignored) {
            }
            this.oldHandler.handle(sig);
        }
    }

    public OpenTelemetryExporter(int prometheusPort,
                                 Arguments args,
                                 Host host,
                                 int hbTimeInterval,
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
        this.dbConnectionState = "Initializing";
        this.counters = counters;
        this.hbTimeInterval = hbTimeInterval;

        if(this.debug) {
            this.printDebug("Creating OpenTelemetryExporter");
        }

        this.openTelemetrySdk = this.initOpenTelemetry();
        Meter openTelemetryMeter = this.openTelemetrySdk.getMeter(SCOPE_NAME);

        if(this.debug) {
            this.printDebug("Creating Metrics");
        }

        this.openTelemetryHBGauge =
                openTelemetryMeter
                    .gaugeBuilder(METRIC_NAME + ".heartbeat")
                    .ofLongs()
                    .setDescription("Aerospike Benchmark MRT HB")
                        .build();

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

        this.hbAttributes = new Attributes[] {
                Attributes.of(
                        AttributeKey.stringKey("generalInfo"), generalInfo.toString(),
                        AttributeKey.stringKey("policies"), policies.toString(),
                        AttributeKey.stringKey("otherInfo"), otherInfo.toString(),
                        AttributeKey.longKey("startTimeMillis"), this.startTimeMillis,
                        AttributeKey.stringKey("startDateTime"), this.startLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                ),
                Attributes.of(
                        AttributeKey.stringKey("DBHost"), host.toString(),
                        AttributeKey.stringArrayKey("namespaces"), args.batchSize > 0 ? Arrays.asList(args.batchNamespaces) : Collections.singletonList(args.namespace),
                        AttributeKey.stringKey("namespace"), args.namespace == null ? args.batchNamespaces[0] : args.namespace,
                        AttributeKey.stringKey("setname"), args.setName,
                        AttributeKey.stringKey("workload"), args.workload.name(),
                        AttributeKey.longKey("batchSize"), (long) args.batchSize
                )};

        if(this.debug) {
            this.printDebug("Creating Signal Handler...");
        }

        OTelSignalHandler handler = new OTelSignalHandler(this);
        Signal.handle(new Signal("TERM"), handler); // Catch SIGTERM
        Signal.handle(new Signal("INT"), handler);  // Catch SIGINT (Ctrl+C)

        if(this.debug) {
            this.printDebug("Creating Timer");
        }

        //Originally went with the observer/callback/async pattern but the callback interval was being influenced by the benchmark app.
        //  This ensures proper heartbeat interval being sent...
        //  THB is more important for long running executions...
        this.hbTimer = new Timer();
        this.hbTimerTask = new HBTimerTask(this);
        if(this.hbTimeInterval > 0) {
            this.hbTimer.schedule(this.hbTimerTask, 1, this.hbTimeInterval);
        }
        else {
            this.updateHBGauge();
        }
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
    private void printMsg(String msg) {
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(debugDateFormatter);

        System.out.printf("%s %s%n", formattedDateTime, msg);
    }

    private void updateHBGauge() {
        AttributesBuilder attributes = Attributes.builder();

        for (Attributes attrItem : this.hbAttributes) {
            attributes.putAll(attrItem);
        }
        attributes.put("hbCount", this.hbCnt.incrementAndGet());

        if(this.clusterName != null) {
            attributes.put("cluster", this.clusterName);
        }
        if(this.dbConnectionState != null) {
            attributes.put("DBConnState", this.dbConnectionState);
        }
        if(this.endTimeMillis != 0) {
            attributes.put("endTimeMillis", this.endTimeMillis);
            attributes.put("endLocalDateTime", this.endLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }

        this.openTelemetryHBGauge.set(System.currentTimeMillis(), attributes.build());

        if(this.debug) {
            this.printDebug(String.format("HeartBeat %d", hbCnt.get()));
        }
    }

    @Override
    public void addException(Exception exception, LatencyTypes type) {

        if(this.closed.get()) { return; }

        String exceptionType = exception.getClass().getName().replaceFirst("com\\.aerospike\\.client\\.AerospikeException\\$", "");
        exceptionType = exceptionType.replaceFirst("com\\.aerospike\\.client\\.", "");

        final Attributes attributes = Attributes.of(
                AttributeKey.stringKey("exception_type"), exceptionType,
                AttributeKey.stringKey("exception"), exception.getMessage(),
                AttributeKey.stringKey("type"), type.name().toLowerCase()
        );

        this.openTelemetryExceptionCounter.add(1, attributes);

        if(this.debug) {
            this.printDebug("Exception Counter Add " + attributes.get(AttributeKey.stringKey("exception_type")));
        }
    }

    @Override
    public void recordElapsedTime(LatencyTypes type, long elapsedMS) {

        if(this.closed.get()) { return; }

        final Attributes attributes = Attributes.of(
            AttributeKey.stringKey("type"), type.name().toLowerCase()
        );

        this.openTelemetryLatencyHistogram.record(elapsedMS, attributes);

        if(this.debug) {
            this.printDebug("Elapsed Time Record  " + attributes.get(AttributeKey.stringKey("type")), true);
        }
    }

    @Override
    public void incrTransCounter(LatencyTypes type) {

        if(this.closed.get()) { return; }

        final Attributes attributes = Attributes.of(
                AttributeKey.stringKey("type"), type.name().toLowerCase()
        );

        this.openTelemetryTransactionCounter.add(1, attributes);

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
        this.updateHBGauge();
    }

    @Override
    public void setDBConnectionState(String dbConnectionState){
        this.dbConnectionState = dbConnectionState;
        if(this.debug) {
            this.printDebug("DB Status Change  " + dbConnectionState, false);
        }
        this.updateHBGauge();
    }

    private void setDBConnectionStateClosed() throws InterruptedException {
        this.endTimeMillis = System.currentTimeMillis();
        this.endLocalDateTime = LocalDateTime.now();
        try {
            if (hbTimer != null) {
                if (this.debug) {
                    this.printDebug("Cancelling HB Timer");
                }
                this.hbTimerTask.cancel();
                this.hbTimer.cancel();
            }
            if (openTelemetryHBGauge != null && !this.aborted.get()) {
                this.printMsg("Sending OpenTelemetry Last Updated Metrics...");
                this.updateHBGauge();
                Thread.sleep(this.hbTimeInterval);
            }
        }
        finally {
            closed.set(true);
        }
    }

    @Override
    public boolean getClosed() { return this.closed.get(); }

    @Override
    public void close() throws Exception {

        if(this.closed.get()) { return; }

        this.printMsg("Closing OpenTelemetry Exporter...");

        this.setDBConnectionStateClosed();

        if(this.counters != null) {
            if (this.debug) {
                this.printDebug("Removing association with counters...");
            }
            this.counters.setOpenTelemetry(null);
        }

        if(openTelemetrySdk != null) {
            if (this.debug) {
                this.printDebug("openTelemetrySdk close....");
            }
            this.openTelemetrySdk.close();
        }

        this.printMsg("Closed OpenTelemetry Exporter");
    }

    public String printConfiguration() {
        return String.format("Open Telemetry Enabled at %s\n\tPrometheus Exporter using Port %d\n\tRefresh Interval (secs): %s\n\tScope Name: '%s'\n\tMetric Prefix Name: '%s'",
                this.startLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                this.prometheusPort,
                this.hbTimeInterval > 0 ? this.hbTimeInterval/1000 : "disabled",
                SCOPE_NAME,
                METRIC_NAME);
    }

    @Override
    public String toString() {
        return String.format("OpenTelemetryExporter{prometheusport:%d, state:%s, interval:%d, heartbeat:%s, transactioncounter:%s, exceptioncounter:%s, latancyhistogram:%s closed:%s}",
                                this.prometheusPort,
                                this.dbConnectionState,
                                this.hbTimeInterval,
                                this.openTelemetryHBGauge,
                                this.openTelemetryTransactionCounter,
                                this.openTelemetryExceptionCounter,
                                this.openTelemetryLatencyHistogram,
                                this.closed.get());
    }
}
