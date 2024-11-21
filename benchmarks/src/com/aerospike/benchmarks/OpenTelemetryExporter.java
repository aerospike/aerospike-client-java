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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public final class OpenTelemetryExporter implements com.aerospike.benchmarks.OpenTelemetry {

    private final String SCOPE_NAME = "com.aerospike.benchmarks";
    private final String METRIC_NAME = "aerospike.benchmarks.mrt";
    private static final double NS_TO_MS = 1000000D;

    private final int prometheusPort;
    private String clusterName;
    private String dbConnectionState;
    private final CounterStore counters;

    private final Attributes[] hbAttributes;
    private final OpenTelemetrySdk openTelemetrySdk;
    private final LongGauge openTelemetryInfoGauge;
    private final LongCounter openTelemetryExceptionCounter;
    private final LongCounter openTelemetryTransactionCounter;
    private final DoubleHistogram openTelemetryLatencyHistogram;

    private final AtomicInteger hbCnt = new AtomicInteger();
    private final long startTimeMillis;
    private final LocalDateTime startLocalDateTime;
    private long endTimeMillis;
    private LocalDateTime endLocalDateTime;
    private final boolean debug;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean aborted = new AtomicBoolean(false);

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
                this.exporter.setDBConnectionStateAbort("SIG" + sig.getName());
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
                                 String clusterName,
                                 StringBuilder generalInfo,
                                 StringBuilder policies,
                                 StringBuilder otherInfo,
                                 long nKeys,
                                 int nthreads,
                                 long nbrMRTs,
                                 boolean asyncEnabled,
                                 CounterStore counters) {
        this.debug = args.debug;
        this.clusterName = clusterName;
        this.startTimeMillis = System.currentTimeMillis();
        this.startLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(this.startTimeMillis),
                                    ZoneId.systemDefault());
        this.prometheusPort = prometheusPort;
        this.dbConnectionState = "Initializing";
        this.counters = counters;

        if(this.debug) {
            this.printDebug("Creating OpenTelemetryExporter");
        }

        this.openTelemetrySdk = this.initOpenTelemetry();
        Meter openTelemetryMeter = this.openTelemetrySdk.getMeter(SCOPE_NAME);

        if(this.debug) {
            this.printDebug("Creating Metrics");
        }

        this.openTelemetryInfoGauge =
                openTelemetryMeter
                        .gaugeBuilder(METRIC_NAME + ".stateinfo")
                        .setDescription("Aerospike Benchmark MRT Config/State Information")
                        .ofLongs()
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

        if(this.debug) {
            this.printDebug("Updating Gauge");
        }

        this.hbAttributes = new Attributes[] {
                Attributes.of(
                        AttributeKey.stringKey("generalInfo"), generalInfo.toString(),
                        AttributeKey.stringKey("policies"), policies.toString(),
                        AttributeKey.stringKey("otherInfo"), otherInfo.toString(),
                        AttributeKey.longKey("startTimeMillis"), this.startTimeMillis,
                        AttributeKey.stringKey("startDateTime"), this.startLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                        AttributeKey.stringKey("DBHost"), host.toString()
                        ),
                Attributes.of(
                        //AttributeKey.stringArrayKey("namespaces"), args.batchSize > 0 ? Arrays.asList(args.batchNamespaces) : Collections.singletonList(args.namespace),
                        AttributeKey.stringKey("namespace"), args.namespace == null ? args.batchNamespaces[0] : args.namespace,
                        AttributeKey.stringKey("setname"), args.setName,
                        AttributeKey.stringKey("workload"), args.workload.name(),
                        AttributeKey.longKey("batchSize"), (long) args.batchSize,
                        AttributeKey.longKey("readPct"), (long) args.readPct,
                        AttributeKey.longKey("readMultiBinPct"), (long) args.readMultiBinPct
                        ),
                Attributes.of(
                        AttributeKey.longKey("writeMultiBinPct"), (long) args.writeMultiBinPct,
                        AttributeKey.longKey("throughputThrottle"), (long) args.throughput,
                        AttributeKey.longKey("transactionLimit"), args.transactionLimit,
                        AttributeKey.longKey("threads"), (long) nthreads,
                        AttributeKey.longKey("nbrMRTs"), nbrMRTs <= 0 ? 0L : nbrMRTs,
                        AttributeKey.longKey("mrtSize"), nbrMRTs <= 0 ? 0L : nbrMRTs / (long) nthreads
                ),
                Attributes.of(
                        AttributeKey.booleanKey("asyncEnabled"), asyncEnabled,
                        AttributeKey.longKey("nkeys"), nKeys <= 0 ? 0L : nKeys,
                        AttributeKey.stringKey("commandlineargs"), String.join(" ", args.commandLineArgs)
                )};

        this.updateInfoGauge(true);

        if(this.debug) {
            this.printDebug("Creating Signal Handler...");
        }

        OTelSignalHandler handler = new OTelSignalHandler(this);
        Signal.handle(new Signal("TERM"), handler); // Catch SIGTERM
        Signal.handle(new Signal("INT"), handler);  // Catch SIGINT (Ctrl+C)
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

    private void updateInfoGauge(boolean initial) {

        if(this.closed.get()) { return; }

        AttributesBuilder attributes = Attributes.builder();


        attributes.put("hbCount", this.hbCnt.incrementAndGet());

        if(initial) {
            attributes.put("type", "static");
            for (Attributes attrItem : this.hbAttributes) {
                attributes.putAll(attrItem);
            }
        }

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

        this.openTelemetryInfoGauge.set(System.currentTimeMillis(), attributes.build());

        if(this.debug) {
            this.printDebug(String.format("Info Gauge %d", hbCnt.get()));
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
                AttributeKey.stringKey("type"), type.name().toLowerCase(),
                AttributeKey.longKey("startTimeMillis"), this.startTimeMillis
        );

        this.openTelemetryExceptionCounter.add(1, attributes);

        if(this.debug) {
            this.printDebug("Exception Counter Add " + attributes.get(AttributeKey.stringKey("exception_type")));
        }
    }

    @Override
    public void recordElapsedTime(LatencyTypes type, long elapsedNanos) {
        this.recordElapsedTime(type, elapsedNanos / NS_TO_MS);
    }

    @Override
    public void recordElapsedTime(LatencyTypes type, double elapsedMS) {

        if(this.closed.get()) { return; }

        final Attributes attributes = Attributes.of(
                AttributeKey.stringKey("type"), type.name().toLowerCase(),
                AttributeKey.longKey("startTimeMillis"), this.startTimeMillis
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
        this.updateInfoGauge(false);
    }

    @Override
    public void setDBConnectionState(String dbConnectionState){
        this.dbConnectionState = dbConnectionState;
        if(this.debug) {
            this.printDebug("DB Status Change  " + dbConnectionState, false);
        }
        this.updateInfoGauge(false);
    }

    private void setDBConnectionStateAbort(String state) throws InterruptedException {
        this.dbConnectionState = state;
        this.endTimeMillis = System.currentTimeMillis();
        this.endLocalDateTime = LocalDateTime.now();
        try {
            if (this.debug) {
                this.printDebug("DB Status Change  " + state, false);
            }
            this.printMsg("Sending OpenTelemetry Last Updated Metrics...");
            this.updateInfoGauge(false);
            Thread.sleep(6000);
        }
        finally {
            this.aborted.set(true);
        }
    }

    private void setDBConnectionStateClosed() throws InterruptedException {
        this.endTimeMillis = System.currentTimeMillis();
        this.endLocalDateTime = LocalDateTime.now();
        try {
            if (openTelemetryInfoGauge != null && !this.aborted.get()) {
                this.printMsg("Sending OpenTelemetry Last Updated Metrics...");
                this.updateInfoGauge(false);
                Thread.sleep(5000); //need to wait for POM to re-scrap...
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
        return String.format("Open Telemetry Enabled at %s\n\tPrometheus Exporter using Port %d\n\tScope Name: '%s'\n\tMetric Prefix Name: '%s'",
                this.startLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                this.prometheusPort,
                SCOPE_NAME,
                METRIC_NAME);
    }

    @Override
    public String toString() {
        return String.format("OpenTelemetryExporter{prometheusport:%d, state:%s, Gauge:%s, transactioncounter:%s, exceptioncounter:%s, latancyhistogram:%s closed:%s}",
                                this.prometheusPort,
                                this.dbConnectionState,
                                this.openTelemetryInfoGauge,
                                this.openTelemetryTransactionCounter,
                                this.openTelemetryExceptionCounter,
                                this.openTelemetryLatencyHistogram,
                                this.closed.get());
    }
}
