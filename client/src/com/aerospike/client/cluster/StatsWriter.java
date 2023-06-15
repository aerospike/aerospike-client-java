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
package com.aerospike.client.cluster;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.async.EventLoopStats;
import com.aerospike.client.listener.StatsListener;
import com.aerospike.client.policy.StatsPolicy;
import com.aerospike.client.util.Util;

public final class StatsWriter implements StatsListener {
	private static final SimpleDateFormat SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private final String dir;
	private final StringBuilder sb;
	private FileWriter writer;
	private boolean enabled;

	public StatsWriter(String dir) {
		this.dir = dir;
		this.sb = new StringBuilder(8192);
	}

	@Override
	public void onEnable(StatsPolicy policy) {
		try {
			String path = dir + File.separator + "stats.txt";
			writer = new FileWriter(path, true);
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}

		sb.setLength(0);
		sb.append(SimpleDateFormat.format(Calendar.getInstance().getTime()));
		sb.append(" header cluster[cpu,mem,threadsInUse,recoverCount,invalidNodeCount,eventloop[],node[]] eventloop[processSize,queueSize] node[name,address,port,connInUse,connInPool,connOpened,connClosed,errors,timeouts,latency[]]");
		sb.append(" latency[");
		sb.append(policy.latencyColumns);
		sb.append(',');
		sb.append(policy.latencyShift);
		sb.append(']');
		writeLine(sb);

		enabled = true;
	}

	@Override
	public void onStats(ClusterStats stats) {
		synchronized(this) {
			if (enabled) {
				writeCluster(stats);
			}
		}
	}

	@Override
	public void onNodeClose(NodeStats stats) {
		synchronized(this) {
			if (enabled) {
				sb.setLength(0);
				sb.append(SimpleDateFormat.format(Calendar.getInstance().getTime()));
				sb.append(" node");
				writeNode(stats);
				writeLine(sb);
			}
		}
	}

	@Override
	public void onDisable(ClusterStats stats) {
		synchronized(this) {
			if (enabled) {
				try {
					enabled = false;
					writeCluster(stats);
					writer.close();
				}
				catch (Throwable e) {
					Log.error("Failed to close metrics writer: " + Util.getErrorMessage(e));
				}
			}
		}
	}

	private void writeCluster(ClusterStats stats) {
		double cpu = Util.getProcessCpuLoad();
		long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		sb.setLength(0);
		sb.append(SimpleDateFormat.format(Calendar.getInstance().getTime()));
		sb.append(" cluster[");
		sb.append((int)cpu);
		sb.append(',');
		sb.append(mem);
		sb.append(',');
		sb.append(stats.threadsInUse);
		sb.append(',');
		sb.append(stats.recoverQueueSize);
		sb.append(',');
		sb.append(stats.invalidNodeCount);
		sb.append(",[");

		EventLoopStats[] eventLoops = stats.eventLoops;

		if (eventLoops != null) {
			for (int i = 0; i < eventLoops.length; i++) {
				EventLoopStats els = eventLoops[i];

				if (i > 0) {
					sb.append(',');
				}

				sb.append('[');
				sb.append(els.processSize);
				sb.append(',');
				sb.append(els.queueSize);
				sb.append(']');
			}
		}
		sb.append("],[");

		NodeStats[] nodes = stats.nodes;

		for (int i = 0; i < nodes.length; i++) {
			NodeStats node = nodes[i];

			if (i > 0) {
				sb.append(',');
			}
			writeNode(node);
		}
		sb.append("]]");
		writeLine(sb);
	}

	private void writeNode(NodeStats ns) {
		Node node = ns.node;
		Host host = node.getHost();

		sb.append('[');
		sb.append(node.getName());
		sb.append(',');
		sb.append(host.name);
		sb.append(',');
		sb.append(host.port);
		writeConn(ns.sync);
		writeConn(ns.async);
		sb.append(',');
		sb.append(ns.errors);
		sb.append(',');
		sb.append(ns.timeouts);
		sb.append(",[");

		int[][] latency = ns.latency;
		int max = LatencyType.getMax();
		boolean comma = false;

		for (int i = 0; i < max; i++) {
			int[] buckets = latency[i];

			if (buckets != null) {
				if (comma) {
					sb.append(',');
				}
				else {
					comma = true;
				}

				sb.append(LatencyType.getString(i));
				sb.append('[');

				for (int j = 0; j < buckets.length; j++) {
					if (i > 0) {
						sb.append(',');
					}
					sb.append(buckets[j]);
				}
				sb.append(']');
			}
		}
		sb.append("]]");
	}

	private void writeConn(ConnectionStats cs) {
		sb.append(',');
		sb.append(cs.inUse);
		sb.append(',');
		sb.append(cs.inPool);
		sb.append(',');
		sb.append(cs.opened);
		sb.append(',');
		sb.append(cs.closed);
	}

	private void writeLine(StringBuilder sb) {
		try {
			sb.append(System.lineSeparator());
			writer.write(sb.toString());
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}
}
