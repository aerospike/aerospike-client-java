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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ConnectionStats;
import com.aerospike.client.cluster.LatencyType;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.util.Util;

/**
 * Default metrics listener. This implementation writes periodic metrics snapshots to a file which
 * will later be read and forwarded to OpenTelemetry by a separate offline application.
 */
public final class MetricsWriter implements MetricsListener {
	private static final SimpleDateFormat FilenameFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	private static final SimpleDateFormat TimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private final String dir;
	private final StringBuilder sb;
	private FileWriter writer;
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
		Date now = Calendar.getInstance().getTime();

		try {
			Files.createDirectories(Paths.get(dir));

			String path = dir + File.separator + "metrics-" + FilenameFormat.format(now) + ".log";
			writer = new FileWriter(path, true);
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}

		sb.setLength(0);
		sb.append(TimestampFormat.format(now));
		sb.append(" header(1)");
		sb.append(" cluster[name,cpu,mem,threadsInUse,recoverQueueSize,invalidNodeCount,tranCount,retryCount,delayQueueTimeoutCount,eventloop[],node[]]");
		sb.append(" eventloop[processSize,queueSize]");
		sb.append(" node[name,address,port,syncConn,asyncConn,errors,timeouts,latency[]]");
		sb.append(" conn[inUse,inPool,opened,closed]");
		sb.append(" latency(");
		sb.append(policy.latencyColumns);
		sb.append(',');
		sb.append(policy.latencyShift);
		sb.append(')');
		sb.append("[type[l1,l2,l3...]]");
		writeLine(sb);

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
				sb.append(TimestampFormat.format(Calendar.getInstance().getTime()));
				sb.append(" node");
				writeNode(node);
				writeLine(sb);
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

	private void writeCluster(Cluster cluster) {
		String clusterName = cluster.getClusterName();

		if (clusterName == null) {
			clusterName = "";
		}

		double cpu = Util.getProcessCpuLoad();
		long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		sb.setLength(0);
		sb.append(TimestampFormat.format(Calendar.getInstance().getTime()));
		sb.append(" cluster[");
		sb.append(clusterName);
		sb.append(',');
		sb.append((int)cpu);
		sb.append(',');
		sb.append(mem);
		sb.append(',');
		sb.append(cluster.getThreadsInUse());
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
		writeLine(sb);
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

	private void writeLine(StringBuilder sb) {
		try {
			sb.append(System.lineSeparator());
			writer.write(sb.toString());
			writer.flush();
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}
}
