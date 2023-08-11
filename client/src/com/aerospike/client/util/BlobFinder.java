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
package com.aerospike.client.util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.ScanPolicy;

/**
 * Find language specific blobs and display counts to the console.
 * Optionally, write each bin that contains a language specific blob.
 */
public final class BlobFinder implements ScanCallback {
	public static BlobFinder INSTANCE;

	/**
	 * Find language specific blobs and display counts to the console.
	 * If path is not null, write each bin that contains a language specific blob(s) to that
	 * file path.
	 *
	 * File format:
	 * <pre>{@code
	 * Blob bins:
	 * <namespace>,<set>,<key hex digest>,<bin name>,<bin type>
	 *
	 * List/Map bins (list and map entries can contain blobs):
	 * <namespace>,<set>,<key hex digest>,<bin name>,<bin type>,<jblobs>,<cblobs>,<pblobs>
	 *
	 * bin type: jblob | cblob | pblob | list | map
	 *
	 * jblobs: count of java blobs in the list or map entries
	 * cblobs: count of C# blobs in the list or map entries
	 * pblobs: count of python blobs in the list or map entries
	 * }</pre>
	 *
	 * @param client		Aerospike client instance
	 * @param path			optional file path. If not null, write all bins that contain language
	 * 						specific blobs.
	 * @param displayRecs	display running blob totals after records returned counter reaches this
	 * 						value. Minimum value is 100000 (display blob totals after each group of
	 * 						100000 records). Final blob totals are always displayed on completion.
	 */
	public static void run(IAerospikeClient client, String path, long displayRecs) throws IOException {
		INSTANCE = new BlobFinder(client, path, displayRecs);
		INSTANCE.run();
	}

	private final IAerospikeClient client;
	private final StringBuilder sb;
	private final FileWriter writer;
	private final long displayRecs;
	private String namespace;
	private long recCount;
	private long javaBlobs;
	private long csharpBlobs;
	private long pythonBlobs;

	private BlobFinder(IAerospikeClient client, String path, long displayRecs) throws IOException {
		this.client = client;
		this.displayRecs = (displayRecs < 100000) ? 100000 : displayRecs;

		if (path != null) {
			this.sb = new StringBuilder(8192);
			this.writer = new FileWriter(path, false);
		}
		else {
			this.sb = null;
			this.writer = null;
		}
	}

	private void run() throws IOException {
		Cluster cluster = client.getCluster();

		HashMap<String,Partitions> pmap = cluster.partitionMap;

		List<String> namespaces = new ArrayList<String>(pmap.size());

		for (String ns : pmap.keySet()) {
			namespaces.add(ns);
		}

		// Set concurrentNodes to false, so atomics are not required in the scan callback.
		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = false;

		for (String ns : namespaces) {
			this.namespace = ns;
			System.out.println("Scan " + ns);
			client.scanAll(policy, ns, null, this);
		}

		displayRunningTotal();

		if (sb != null) {
			writer.close();
		}
	}

	@Override
	public void scanCallback(Key key, Record record) {
		recCount++;

		if (recCount % displayRecs == 0) {
			displayRunningTotal();
		}
	}

	private void displayRunningTotal() {
		System.out.println("recs=" + recCount +
			" jblobs=" + javaBlobs +
			" cblobs=" + csharpBlobs +
			" pblobs=" + pythonBlobs
			);
	}

	/**
	 * Write language specific blob bin.
	 *
	 * @param digest	key digest
	 * @param binName	bin name
	 * @param type		particle type integer
	 * 					7: Java blob
	 * 					8: C# blob
	 * 					9: Python blob
	 */
	public void writeBin(Key key, String binName, int type) {
		String btype = null;

		switch (type) {
		case ParticleType.JBLOB:
			btype = "jblob";
			this.javaBlobs++;
			break;

		case ParticleType.CSHARP_BLOB:
			btype = "cblob";
			this.csharpBlobs++;
			break;

		case ParticleType.PYTHON_BLOB:
			btype = "pblob";
			this.pythonBlobs++;
			break;
		}

		if (sb == null) {
			return;
		}

		sb.setLength(0);
		sb.append(namespace);
		sb.append(',');
		sb.append(key.setName);
		sb.append(',');
		sb.append(Buffer.bytesToHexString(key.digest));
		sb.append(',');
		sb.append(binName);
		sb.append(',');
		sb.append(btype);
		writeLine();
	}

	/**
	 * Write blob counts that occurred in a list or map bin.
	 *
	 * @param digest		key digest
	 * @param binName		bin name
	 * @param type			"list" or "map"
	 * @param javaCount		count of java blobs in list or map bin.
	 * @param csharpCount	count of csharp blobs in list or map bin.
	 * @param pythonCount	count of python blobs in list or map bin.
	 */
	public void writeListMap (
		Key key,
		String binName,
		String type,
		int javaBlobs,
		int csharpBlobs,
		int pythonBlobs
	) {
		this.javaBlobs += javaBlobs;
		this.csharpBlobs += csharpBlobs;
		this.pythonBlobs += pythonBlobs;

		if (sb == null) {
			return;
		}

		sb.setLength(0);
		sb.append(namespace);
		sb.append(',');
		sb.append(key.setName);
		sb.append(',');
		sb.append(Buffer.bytesToHexString(key.digest));
		sb.append(',');
		sb.append(binName);
		sb.append(',');
		sb.append(type);
		sb.append(',');
		sb.append(javaBlobs);
		sb.append(',');
		sb.append(csharpBlobs);
		sb.append(',');
		sb.append(pythonBlobs);
		writeLine();
	}

	private void writeLine() {
		try {
			sb.append(System.lineSeparator());
			writer.write(sb.toString());
			writer.flush();
		}
		catch (IOException ioe) {
			try {
				writer.close();
			}
			catch (Throwable t) {
			}

			throw new AerospikeException(ioe);
		}
	}
}
