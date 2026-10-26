/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.metrics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

/**
 * Default metrics exporter. This implementation writes periodic metrics snapshots to a file which
 * will later be read and forwarded to OpenTelemetry by a separate offline application.
 */
public final class MetricsWriter implements IMetricsExporter {
	private static final DateTimeFormatter FilenameFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	private static final DateTimeFormatter TimestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	private static final int MinFileSize = 1000000;

	private final String dir;
	private final StringBuilder sb;
	private FileWriter writer;
	private long size;
	private long maxSize;
	private int latencyColumns = 7;
	private int latencyShift = 1;

	/**
	 * Initialize metrics writer.
	 */
	public MetricsWriter(String dir) {
		this.dir = dir;
		this.sb = new StringBuilder(8192);
	}

	/**
	 * Initialize metrics writer from policy.
	 */
	public MetricsWriter(MetricsPolicy policy) {
		this(policy.reportDir);
		configure(policy);
	}

	/**
	 * Apply file rotation and latency histogram settings.
	 */
	void configure(MetricsPolicy policy) {
		if (policy.reportSizeLimit != 0 && policy.reportSizeLimit < MinFileSize) {
			throw new AerospikeException("MetricsPolicy.reportSizeLimit " + policy.reportSizeLimit +
				" must be at least " + MinFileSize);
		}

		this.maxSize = policy.reportSizeLimit;
		this.latencyColumns = policy.latencyColumns;
		this.latencyShift = policy.latencyShift;
	}

	@Override
	public synchronized void export(MetricsSnapshot snapshot) {
		try {
			ensureOpen();
			writeCluster(snapshot);
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}

	/**
	 * Flush and close the client-owned metrics file.
	 */
	public synchronized void closeWriter() {
		if (writer == null) {
			return;
		}

		try {
			writer.flush();
			writer.close();
		}
		catch (Throwable e) {
			Log.error("Failed to close metrics writer: " + Util.getErrorMessage(e));
		}
		finally {
			writer = null;
		}
	}

	private void ensureOpen() throws IOException {
		if (writer == null) {
			Files.createDirectories(Paths.get(dir));
			open();
		}
	}

	private void open() throws IOException {
		LocalDateTime now = LocalDateTime.now();
		String path = dir + File.separator + "metrics-" + now.format(FilenameFormat) + ".log";
		writer = new FileWriter(path, false);
		size = 0;

		// Must use separate StringBuilder instance to avoid conflicting with metrics detail write.
		sb.setLength(0);
		sb.append(now.format(TimestampFormat));
		sb.append(" header(2)");
		sb.append(" cluster[name,clientType,clientVersion,appId,label[],cpu,mem,recoverQueueSize,invalidNodeCount,commandCount,retryCount,delayQueueTimeoutCount,eventloop[],node[]]");
		sb.append(" label[name,value]");
		sb.append(" eventloop[processSize,queueSize]");
		sb.append(" node[name,address,port,syncConn,asyncConn,namespace[]]");
		sb.append(" conn[inUse,inPool,opened,closed]");
		sb.append(" namespace[name,errors,timeouts,keyBusy,bytesIn,bytesOut,latency[]]");
		sb.append(" latency(");
		sb.append(latencyColumns);
		sb.append(',');
		sb.append(latencyShift);
		sb.append(')');
		sb.append("[type[l1,l2,l3...]]");
		writeLine();
	}

	private void writeCluster(MetricsSnapshot snapshot) {
		sb.setLength(0);
		LocalDateTime timestamp = LocalDateTime.ofInstant(snapshot.timestamp, ZoneId.systemDefault());
		sb.append(timestamp.format(TimestampFormat));
		sb.append(" cluster[");
		sb.append(snapshot.clusterName);
		sb.append(',');
		sb.append(snapshot.clientType);
		sb.append(',');
		sb.append(snapshot.clientVersion);
		sb.append(',');
		sb.append(snapshot.appId);
		sb.append(',');
		if (!snapshot.labels.isEmpty()) {
			sb.append("[");
			for (String key : snapshot.labels.keySet()) {
				sb.append("[").append(key).append(",").append(snapshot.labels.get(key)).append("],");
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append("]");
		}
		sb.append(',');
		sb.append((int)snapshot.cpuPercent);
		sb.append(',');
		sb.append(snapshot.memoryBytes);
		sb.append(',');
		sb.append(snapshot.recoverQueueSize);
		sb.append(',');
		sb.append(snapshot.invalidNodeCount);
		sb.append(',');
		sb.append(snapshot.commandCount);
		sb.append(',');
		sb.append(snapshot.retryCount);
		sb.append(',');
		sb.append(snapshot.delayQueueTimeoutCount);
		sb.append(",[");

		if (snapshot.eventLoops != null) {
			for (int i = 0; i < snapshot.eventLoops.size(); i++) {
				EventLoopMetricsSnapshot el = snapshot.eventLoops.get(i);

				if (i > 0) {
					sb.append(',');
				}

				sb.append('[');
				sb.append(el.processSize);
				sb.append(',');
				sb.append(el.queueSize);
				sb.append(']');
			}
		}
		sb.append("],[");

		for (int i = 0; i < snapshot.nodes.size(); i++) {
			NodeMetricsSnapshot node = snapshot.nodes.get(i);

			if (i > 0) {
				sb.append(',');
			}
			writeNode(node);
		}
		sb.append("]");
		writeLine();
	}

	private void writeNode(NodeMetricsSnapshot node) {
		sb.append('[');
		sb.append(node.nodeName);
		sb.append(',');
		sb.append(node.nodeAddress);
		sb.append(',');
		sb.append(node.nodePort);
		sb.append(',');

		writeConn(node.syncConnections);
		sb.append(',');
		writeConn(node.asyncConnections);
		sb.append(",[");

		for (int ns = 0; ns < node.namespaces.size(); ns++) {
			NamespaceMetricsSnapshot namespace = node.namespaces.get(ns);
			sb.append(namespace.namespace).append(',');
			sb.append(namespace.errors);
			sb.append(',');
			sb.append(namespace.clientTimeouts);
			sb.append(',');
			sb.append(namespace.keyBusy);
			sb.append(',');
			sb.append(namespace.bytesIn);
			sb.append(',');
			sb.append(namespace.bytesOut);
			sb.append(",[");
			LatencySnapshot[] latencies = {
				node.connectionLatency,
				namespace.writeLatency,
				namespace.readLatency,
				namespace.batchLatency,
				namespace.queryLatency
			};
			for (int i = 0; i < latencies.length; i++) {
				if (i > 0) {
					sb.append(',');
				}

				sb.append(LatencyType.getString(i));
				sb.append('[');

				long[] buckets = latencies[i] == null ? new long[0] : latencies[i].getBucketCounts();
				for (int j = 0; j < buckets.length; j++) {
					if (j > 0) {
						sb.append(',');
					}
					sb.append(buckets[j]);
				}
				sb.append(']');
			}
			if (ns < node.namespaces.size() - 1) {
				sb.append("]],[");
			} else {
				sb.append("]]");
			}
		}
		sb.append("]]");
	}

	private void writeConn(ConnectionMetricsSnapshot cs) {
		sb.append(cs.inUse);
		sb.append(',');
		sb.append(cs.inPool);
		sb.append(',');
		sb.append(cs.opened); // Cumulative. Not reset on each interval.
		sb.append(',');
		sb.append(cs.closed); // Cumulative. Not reset on each interval.
	}

	private void writeLine() {
		try {
			sb.append(System.lineSeparator());
			writer.write(sb.toString());
			size += sb.length();
			writer.flush();

			if (maxSize > 0 && size >= maxSize) {
				writer.close();

				// This call is recursive since open() calls writeLine() to write the header.
				open();
			}
		}
		catch (IOException ioe) {
			try {
				writer.close();
			}
			catch (Throwable t) {
			}
			finally {
				writer = null;
			}

			throw new AerospikeException(ioe);
		}
	}
}
