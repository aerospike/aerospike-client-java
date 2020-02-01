/*
 * Copyright 2012-2020 Aerospike, Inc.
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
package com.aerospike.test.sync.basic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.test.sync.TestSync;

public class TestScan extends TestSync implements ScanCallback {
	private final Map<String,Metrics> setMap = new HashMap<String,Metrics>();

	@Test
	public void scanParallel() {
		ScanPolicy policy = new ScanPolicy();
		client.scanAll(policy, args.namespace, args.set, this);
	}

	@Test
	public void scanSeries() {
		ScanPolicy policy = new ScanPolicy();
		List<String> nodeList = client.getNodeNames();

		for (String nodeName : nodeList) {
			client.scanNode(policy, nodeName, args.namespace, args.set, this);

			for (Map.Entry<String,Metrics> entry : setMap.entrySet()) {
				entry.getValue().count = 0;
			}
		}
	}

	@Override
	public void scanCallback(Key key, Record record) {
		Metrics metrics = setMap.get(key.setName);

		if (metrics == null) {
			metrics = new Metrics();
		}
		metrics.count++;
		metrics.total++;
		setMap.put(key.setName, metrics);
	}

	public static class Metrics {
		public long count = 0;
		public long total = 0;
	}
}
