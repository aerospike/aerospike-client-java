/*
 * Copyright 2012-2023 Aerospike, Inc.
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
import java.time.format.DateTimeFormatter;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ConnectionStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.util.Util;

/**
 * Default metrics listener. This implementation writes periodic metrics snapshots to a file which
 * will later be read and forwarded to OpenTelemetry by a separate offline application.
 */
public final class MetricsWriter implements MetricsListener {
	private static final DateTimeFormatter FilenameFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	private static final DateTimeFormatter TimestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	private static final int MinFileSize = 1000000;

	private final String dir;
	private final StringBuilder sb;
	private FileWriter writer;
	private long size;
	private long maxSize;
	private int latencyColumns;
	private int latencyShift;
	private boolean enabled;

	/**
	 * Initialize metrics writer.
	 */
	public MetricsWriter(String dir) {
		this.dir = dir;
		this.sb = new StringBuilder(8192);
	}

	/**
	 * Open timestamped metrics file in append mode and write header indicating what metrics will
	 * be stored.
	 */
	@Override
	public void onEnable(Cluster cluster, MetricsPolicy policy) {
		if (policy.reportSizeLimit != 0 && policy.reportSizeLimit < MinFileSize) {
			throw new AerospikeException("MetricsPolicy.reportSizeLimit " + policy.reportSizeLimit +
				" must be at least " + MinFileSize);
		}

		this.maxSize = policy.reportSizeLimit;
		this.latencyColumns = policy.latencyColumns;
		this.latencyShift = policy.latencyShift;

		try {
			Files.createDirectories(Paths.get(dir));
			open();
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}

		enabled = true;
	}

	/**
	 * Write cluster metrics snapshot to file.
	 */
	@Override
	public void onSnapshot(Cluster cluster) {
		synchronized(this) {
			if (enabled) {
				writeCluster(cluster);
			}
		}
	}

	/**
	 * Write final node metrics snapshot on node that will be closed.
	 */
	@Override
	public void onNodeClose(Node node) {
		synchronized(this) {
			if (enabled) {
				sb.setLength(0);
				sb.append(LocalDateTime.now().format(TimestampFormat));
				sb.append(" node");
				writeNode(node);
				writeLine();
			}
		}
	}

	/**
	 * Write final cluster metrics snapshot to file and then close the file.
	 */
	@Override
	public void onDisable(Cluster cluster) {
		synchronized(this) {
			if (enabled) {
				try {
					enabled = false;
					writeCluster(cluster);
					writer.close();
				}
				catch (Throwable e) {
					Log.error("Failed to close metrics writer: " + Util.getErrorMessage(e));
				}
			}
		}
	}

	private void open() throws IOException {
		LocalDateTime now = LocalDateTime.now();
		String path = dir + File.separator + "metrics-" + now.format(FilenameFormat) + ".log";
		writer = new FileWriter(path, true);
		size = 0;

		// Must use separate StringBuilder instance to avoid conflicting with metrics detail write.
		sb.setLength(0);
		sb.append(now.format(TimestampFormat));
		sb.append(" header(1)");
		sb.append(" cluster[name,cpu,mem,recoverQueueSize,invalidNodeCount,tranCount,retryCount,delayQueueTimeoutCount,eventloop[],node[]]");
		sb.append(" eventloop[processSize,queueSize]");
		sb.append(" node[name,address,port,syncConn,asyncConn,errors,timeouts,latency[]]");
		sb.append(" conn[inUse,inPool,opened,closed]");
		sb.append(" latency(");
		sb.append(latencyColumns);
		sb.append(',');
		sb.append(latencyShift);
		sb.append(')');
		sb.append("[type[l1,l2,l3...]]");
		writeLine();
	}

	private void writeCluster(Cluster cluster) {
		String clusterName = cluster.getClusterName();

		if (clusterName == null) {
			clusterName = "";
		}

		double cpu = Util.getProcessCpuLoad();
		long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		sb.setLength(0);
		sb.append(LocalDateTime.now().format(TimestampFormat));
		sb.append(" cluster[");
		sb.append(clusterName);
		sb.append(',');
		sb.append((int)cpu);
		sb.append(',');
		sb.append(mem);
		sb.append(',');
		sb.append(cluster.getRecoverQueueSize());
		sb.append(',');
		sb.append(cluster.getInvalidNodeCount()); // Cumulative. Not reset on each interval.
		sb.append(',');
		sb.append(cluster.getTranCount());  // Cumulative. Not reset on each interval.
		sb.append(',');
		sb.append(cluster.getRetryCount()); // Cumulative. Not reset on each interval.
		sb.append(',');
		sb.append(cluster.getDelayQueueTimeoutCount()); // Cumulative. Not reset on each interval.
		sb.append(",[");

		EventLoop[] eventLoops = cluster.getEventLoopArray();

		if (eventLoops != null) {
			for (int i = 0; i < eventLoops.length; i++) {
				EventLoop el = eventLoops[i];

				if (i > 0) {
					sb.append(',');
				}

				sb.append('[');
				sb.append(el.getProcessSize());
				sb.append(',');
				sb.append(el.getQueueSize());
				sb.append(']');
			}
		}
		sb.append("],[");

		Node[] nodes = cluster.getNodes();

		for (int i = 0; i < nodes.length; i++) {
			Node node = nodes[i];

			if (i > 0) {
				sb.append(',');
			}
			writeNode(node);
		}
		sb.append("]]");
		writeLine();
	}

	private void writeNode(Node node) {
		sb.append('[');
		sb.append(node.getName());
		sb.append(',');

		Host host = node.getHost();

		sb.append(host.name);
		sb.append(',');
		sb.append(host.port);
		sb.append(',');

		writeConn(node.getConnectionStats());
		sb.append(',');
		writeConn(node.getAsyncConnectionStats());
		sb.append(',');

		sb.append(node.getErrorCount());   // Cumulative. Not reset on each interval.
		sb.append(',');
		sb.append(node.getTimeoutCount()); // Cumulative. Not reset on each interval.
		sb.append(",[");

		NodeMetrics nm = node.getMetrics();
		int max = LatencyType.getMax();

		for (int i = 0; i < max; i++) {
			if (i > 0) {
				sb.append(',');
			}

			sb.append(LatencyType.getString(i));
			sb.append('[');

			LatencyBuckets buckets = nm.getLatencyBuckets(i);
			int bucketMax = buckets.getMax();

			for (int j = 0; j < bucketMax; j++) {
				if (j > 0) {
					sb.append(',');
				}
				sb.append(buckets.getBucket(j)); // Cumulative. Not reset on each interval.
			}
			sb.append(']');
		}
		sb.append("]]");
	}

	private void writeConn(ConnectionStats cs) {
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
			enabled = false;

			try {
				writer.close();
			}
			catch (Throwable t) {
			}

			throw new AerospikeException(ioe);
		}
	}
}
