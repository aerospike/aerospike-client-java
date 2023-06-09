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

import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ConnectionStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.NodeStats;
import com.aerospike.client.policy.MetricsPolicy;
import com.aerospike.client.util.Util;

public final class MetricsWriter {
	private static final SimpleDateFormat SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private final StringBuilder sb;
	private final FileWriter writer;
	private final int latencyColumns;
	private final int latencyShift;
	private boolean closed;

	public MetricsWriter(MetricsPolicy policy) {
		latencyColumns = policy.latencyColumns;
		latencyShift = policy.latencyShift;
		sb = new StringBuilder(512);

		try {
			writer = new FileWriter(policy.reportPath, true);
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}

		writeHeader();
	}

	private void writeHeader() {
		sb.setLength(0);
		sb.append(SimpleDateFormat.format(Calendar.getInstance().getTime()));
		sb.append(" header cluster cpu mem threadsInUse recoverCount invalidNodeCount eventloops processSize queueSize node nodeName nodeAddress nodePort connInUse connInPool connOpened connClosed errors timeouts");
		LatencyManager.printHeader(sb, latencyColumns, latencyShift);
		writeLine(sb);
	}

	public void writeCluster(Cluster cluster) {
		synchronized(writer) {
			if (!closed) {
				write(cluster);
			}
		}
	}

	public void writeNode(Node node) {
		synchronized(writer) {
			if (!closed) {
				sb.setLength(0);
				sb.append(Calendar.getInstance().getTime());
				write(node);
				writeLine();
			}
		}
	}

	public void close(Cluster cluster) {
		synchronized(writer) {
			if (!closed) {
				try {
					closed = true;
					write(cluster);
					writer.close();
				}
				catch (Exception e) {
					Log.error("Failed to close metrics writer: " + Util.getErrorMessage(e));
				}
			}
		}
	}

	private void write(Cluster cluster) {
		double cpu = getProcessCpuLoad();
		long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		int threadsInUse = cluster.getThreadsInUse();
		int recoverCount = cluster.getRecoverCount();
		int invalidNodeCount = cluster.getInvalidNodeCount();

		sb.setLength(0);
		sb.append(Calendar.getInstance().getTime());
		sb.append(" cluster ");
		sb.append((int)cpu);
		sb.append(' ');
		sb.append(mem);
		sb.append(' ');
		sb.append(threadsInUse);
		sb.append(' ');
		sb.append(recoverCount);
		sb.append(' ');
		sb.append(invalidNodeCount);
		sb.append(" [");

		EventLoops loops = cluster.eventLoops;

		if (loops != null) {
			EventLoop[] array = loops.getArray();

			for (int i = 0; i < array.length; i++) {
				EventLoop e = array[i];
				sb.append('[');
				sb.append(e.getProcessSize());
				sb.append(',');
				sb.append(e.getQueueSize());
				sb.append(']');
			}
		}
		sb.append(']');
		write(sb);

		Node[] nodes = cluster.getNodes();

		for (Node node : nodes)
		{
			sb.setLength(0);
			write(node);
		}
		writeLine();
	}

	private void write(Node node) {
		NodeStats ns = new NodeStats(node);
		ConnectionStats cs = ns.sync;
		Metrics metrics = node.getMetrics();
		int errors = metrics.resetError();
		int timeouts = metrics.resetTimeout();

		sb.append(" node ");
		sb.append(node);
		sb.append(' ');
		sb.append(cs.inUse);
		sb.append(' ');
		sb.append(cs.inPool);
		sb.append(' ');
		sb.append(cs.opened);
		sb.append(' ');
		sb.append(cs.closed);
		sb.append(' ');
		sb.append(errors);
		sb.append(' ');
		sb.append(timeouts);
		write(sb);

		WriteLatency(node, metrics.get(LatencyType.CONN), "conn");
		WriteLatency(node, metrics.get(LatencyType.WRITE), "write");
		WriteLatency(node, metrics.get(LatencyType.READ), "read");
		WriteLatency(node, metrics.get(LatencyType.BATCH), "batch");
		WriteLatency(node, metrics.get(LatencyType.QUERY), "query");
	}

	private void WriteLatency(Node node, LatencyManager lm, String type) {
		if (lm.printResults(node, sb, type)) {
			write(sb);
		}
	}

	private void write(StringBuilder sb) {
		try {
			writer.write(sb.toString());
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}

	private void writeLine(StringBuilder sb) {
		try {
			writer.write(sb.toString());
			writer.write(System.lineSeparator());
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}

	private void writeLine() {
		try {
			writer.write(System.lineSeparator());
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}

	private static double getProcessCpuLoad() {
		try {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
			AttributeList list = mbs.getAttributes(name, new String[]{ "ProcessCpuLoad" });

			if (list.isEmpty()) {
				return 0.0;
			}

			Attribute att = (Attribute)list.get(0);
			Double value = (Double)att.getValue();

			// usually takes a couple of seconds before we get real values
			if (value == -1.0) {
				return 0.0;
			}

			// returns a percentage value with 1 decimal point precision
			return ((int)(value * 1000) / 10.0);
		}
		catch (Exception e) {
			return 0.0;
		}
	}
}
